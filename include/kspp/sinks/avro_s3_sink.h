#include <memory>
#include <iostream>
#include <fstream>
#include <experimental/filesystem>
#include <avro/Generic.hh>
#include <avro/DataFile.hh>
#include <kspp/avro/avro_utils.h>
#include <kspp/kspp.h>
#include <aws/core/Aws.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/CreateBucketRequest.h>
#include <aws/core/auth/AWSCredentialsProvider.h>
#include <aws/core/http/Scheme.h>
#include <aws/s3/model/ListObjectsRequest.h>
#include <aws/s3/model/PutObjectRequest.h>
#include <kspp/features/aws/aws.h>
#pragma once

namespace kspp {
  template<class V>
  class avro_s3_sink : public topic_sink<void, V> {
    static constexpr const char* PROCESSOR_NAME = "avro_s3_sink";
  public:
    avro_s3_sink(std::shared_ptr<cluster_config> config, kspp::url uri, std::string key_base, std::chrono::seconds window_size)
        : topic_sink<void, V>()
        , key_base_(key_base)
        , window_size_(window_size) {
      assert(uri.scheme() == "s3");
      //todo assumes that path starts with / - add checks
      std::string path_without_slash = uri.path().substr(1);
      bucket_ = path_without_slash.substr(0, path_without_slash.find("/"));
      if (bucket_.empty()) {
        LOG(ERROR) << "bad s3 bucket: " << uri.str();
        LOG(FATAL) << "bad s3 bucket: " << uri.str();
      }

      size_t key_start = bucket_.size() + 2;
      key_prefix_ = uri.path().substr(key_start);
      if (key_prefix_.empty()) {
        LOG(ERROR) << "bad s3 key prefix: " << uri.str();
        LOG(FATAL) << "bad s3 key prefix: " << uri.str();
      }

      LOG(INFO) << "s3: " << uri.authority() << ", bucket: " << bucket_ << ", key prefix: " << key_prefix_;

      // todo get this from cluster config???
      std::string access_key = getenv("S3_ACCESS_KEY_ID");
      std::string secret_key = getenv("S3_SECRET_ACCESS_KEY");

      LOG(INFO) << "S3_ACCESS_KEY_ID: " << access_key;
      if (access_key.empty()) {
        LOG(ERROR) << "S3_ACCESS_KEY_ID not defined";
        LOG(FATAL) << "S3_ACCESS_KEY_ID not defined";
      }

      if (secret_key.empty()) {
        LOG(ERROR) << "S3_SECRET_ACCESS_KEY not defined";
        LOG(FATAL) << "S3_SECRET_ACCESS_KEY not defined";
      }


      kspp::init_aws(); // must be done at least once - otherwise the aws functions segfaults

      std::string host = uri.authority();
      // this is just to check if the host is a ip address or a name - not that this only works for ipv4
      bool use_ssl = true;
      std::string host_without_port = host.substr(0, host.find(':'));
      boost::system::error_code ec;
      boost::asio::ip::address::from_string(host_without_port, ec);
      if (!ec) { // is we get an ip addres here - lets disable ssl
        use_ssl = false;
        LOG(WARNING) << "disabling SSL for " << host;
      }

      Aws::Client::ClientConfiguration aws_config;

      aws_config.endpointOverride = Aws::String(host.c_str());
      aws_config.scheme = use_ssl ? Aws::Http::Scheme::HTTPS : Aws::Http::Scheme::HTTP;
      aws_config.connectTimeoutMs = 5000;
      aws_config.requestTimeoutMs = 1000;

      s3_client_ = std::make_shared<Aws::S3::S3Client>(
          Aws::Auth::AWSCredentials(Aws::String(access_key.c_str()), Aws::String(secret_key.c_str())),
          aws_config,
          Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never,
          false);

      this->add_metrics_label(KSPP_PROCESSOR_TYPE_TAG, PROCESSOR_NAME);
    }

    ~avro_s3_sink() override {
      this->flush();
      this->close();
    }

    void close() override {
      close_file();
      LOG(INFO) << PROCESSOR_NAME << " processor closed - consumed " << this->_processed_count.value() << " messages";
    }

    void close_file() {
      if (file_writer_) {
        file_writer_->flush();
        file_writer_->close();
        file_writer_.reset();
        LOG(INFO) << PROCESSOR_NAME << ", file: "  << current_file_name_ << " closed - written " << messages_in_file_ << " messages";
        LOG(INFO) << PROCESSOR_NAME << ", file: "  << current_file_name_ << " uploading to S3 " << current_s3_file_name_;
        upload(current_file_name_, current_s3_file_name_);
        std::experimental::filesystem::remove(current_file_name_);
        LOG(INFO) << PROCESSOR_NAME << ", file: "  << current_file_name_ << " deleted";
      }
    }

    std::string log_name() const override {
      return PROCESSOR_NAME;
    }

    size_t queue_size() const override {
      return event_consumer<void, V>::queue_size();
    }

    void flush() override {
      while (process(kspp::milliseconds_since_epoch())>0)
      {
        ; // noop
      }
    }

    bool eof() const override {
      return this->_queue.size();
    }

    size_t process(int64_t tick) override {
      size_t processed =0;

      //forward up this timestamp
      while (this->_queue.next_event_time()<=tick){
        auto r = this->_queue.pop_front_and_get();
        this->_lag.add_event_time(tick, r->event_time());

        // check if it's time to rotate
        if (file_writer_ && r->event_time()>= end_of_window_ts_){
          close_file();
        }

        // time to create a new file?
        if (!file_writer_){
          auto schema = avro_utils::avro_utils<V>::valid_schema(*r->record()->value());
          current_file_name_ = std::tmpnam(nullptr);
          current_file_name_ += ".avro";
          current_s3_file_name_ = key_prefix_ + "/" + key_base_ + "-" + std::to_string(r->event_time()) + ".avro";
          end_of_window_ts_ =  r->event_time() + (window_size_.count() * 1000); // should we make another kind of window that plays nice with 24h?
          messages_in_file_ = 0;
#ifdef SNAPPY_CODEC_AVAILABLE
          file_writer_ = std::make_shared<avro::DataFileWriter<V>>(current_file_name_.c_str(), *schema, 16 * 1024, avro::SNAPPY_CODEC);
#else
          _file_writer = std::make_shared<avro::DataFileWriter<V>>(_current_file_name.c_str(), *schema, 16 * 1024, avro::DEFLATE_CODEC);
#endif
        }

        // null value protection
        if (r->record()->value()) {
          ++messages_in_file_;
          file_writer_->write(*r->record()->value());
        }
        ++(this->_processed_count);
        ++processed;
      }
      return processed;
    }

  protected:
    bool upload(std::string src,  std::string destination){
        Aws::S3::Model::PutObjectRequest object_request;
        Aws::String aws_bucket_name(bucket_.c_str()); // GCC?
        Aws::String aws_key_name(destination.c_str()); // GCC?
        object_request.WithBucket(aws_bucket_name).WithKey(aws_key_name);

        // Binary files must also have the std::ios_base::bin flag or'ed in
        const std::shared_ptr<Aws::IOStream> input_data = Aws::MakeShared<Aws::FStream>("PutObjectInputStream", src.c_str(), std::ios_base::in | std::ios_base::binary);
        object_request.SetBody(input_data);

        LOG(INFO) << "S3 PUT BEGIN";
        auto put_object_outcome = s3_client_->PutObject(object_request);
        if (put_object_outcome.IsSuccess())
        {
          LOG(INFO) << "S3 " << destination << " done";
          return true;
        }
        else
        {
          LOG(ERROR) << "S3 PutObject error: " << put_object_outcome.GetError().GetExceptionName() << " " << put_object_outcome.GetError().GetMessage() << ", key_name: " << destination << ", fname: " << src;
          return false;
        }
    }

    std::shared_ptr<avro::DataFileWriter<V>> file_writer_;
    std::string current_file_name_;
    std::string current_s3_file_name_;
    std::shared_ptr<Aws::S3::S3Client> s3_client_;
    std::string bucket_;
    std::string key_prefix_;
    const std::string key_base_;
    const std::chrono::seconds window_size_;
    int64_t end_of_window_ts_=0;
    int64_t messages_in_file_=0;
  };
}
