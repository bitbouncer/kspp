#include <kspp/features/aws/s3_offset_storage_provider.h>
#include <boost/interprocess/streams/bufferstream.hpp>
#include <aws/core/Aws.h>
#include <aws/core/auth/AWSCredentialsProvider.h>
#include <aws/s3/model/PutObjectRequest.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <kspp/kspp.h>
#include <kspp/features/aws/aws.h>

namespace kspp {

  std::shared_ptr<s3_offset_storage> s3_offset_storage::create(kspp::url uri) {
    assert(uri.scheme() == "s3");

    //todo assumes that path starts with / - add checks
    std::string path_without_slash = uri.path().substr(1);
    std::string bucket = path_without_slash.substr(0, path_without_slash.find("/"));
    if (bucket.empty()) {
      LOG(ERROR) << "bad s3 bucket";
      return nullptr;
    }

    size_t key_start = bucket.size() + 2;
    std::string key = uri.path().substr(key_start);
    if (key.empty()) {
      LOG(ERROR) << "bad s3 key";
      return nullptr;
    }

    LOG(INFO) << "s3: " << uri.authority() << ", bucket: " << bucket << ", key: " << key;

    // todo get this from cluster config???
    std::string access_key = getenv("S3_ACCESS_KEY_ID");
    std::string secret_key = getenv("S3_SECRET_ACCESS_KEY");

    LOG(INFO) << "S3_ACCESS_KEY_ID: " << access_key;
    if (access_key.empty()) {
      LOG(ERROR) << "S3_ACCESS_KEY_ID not defined";
      return nullptr;
    }

    if (secret_key.empty()) {
      LOG(ERROR) << "S3_SECRET_ACCESS_KEY not defined";
      return nullptr;
    }

    return std::make_shared<s3_offset_storage>(uri.authority(), bucket, key, access_key, secret_key);
  }

  s3_offset_storage::s3_offset_storage(std::string host, std::string s3_bucket, std::string s3_object_name,
                                       std::string access_key, std::string secret_key)
      : s3_bucket_(s3_bucket), s3_object_name_(s3_object_name) {

    kspp::init_aws(); // must be done at least once - otherwise the aws functions segfaults

    // this is just to check if the host is a ip address or a name - not that this only works for ipv4
    bool use_ssl = true;
    std::string host_without_port = host.substr(0, host.find(':'));
    boost::system::error_code ec;
    boost::asio::ip::address::from_string(host_without_port, ec);
    if (!ec) { // is we get an ip addres here - lets disable ssl
      use_ssl = false;
      LOG(WARNING) << "disabling SSL for " << host;
    }


    Aws::Client::ClientConfiguration config;

    config.endpointOverride = Aws::String(host.c_str());
    config.scheme = use_ssl ? Aws::Http::Scheme::HTTPS : Aws::Http::Scheme::HTTP;
    config.connectTimeoutMs = 5000;
    config.requestTimeoutMs = 1000;

    s3_client_ = std::make_shared<Aws::S3::S3Client>(
        Aws::Auth::AWSCredentials(Aws::String(access_key.c_str()), Aws::String(secret_key.c_str())),
        config,
        Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never,
        false);
  }

  s3_offset_storage::~s3_offset_storage() {
    persist_offset(last_commited_offset_, flush_offset_timeout_ms_);
    //S3_deinitialize();
  }

  int64_t s3_offset_storage::load_offset(int timeout_ms) {
    Aws::S3::Model::GetObjectRequest object_request;
    object_request.SetBucket(Aws::String(s3_bucket_.c_str()));
    object_request.SetKey(Aws::String(s3_object_name_.c_str()));
    object_request.SetRange("0-7");

    auto get_object_outcome = s3_client_->GetObject(object_request);
    if (get_object_outcome.IsSuccess()) {
      auto &retrieved_data = get_object_outcome.GetResultWithOwnership().GetBody();
      int64_t tmp = kspp::OFFSET_BEGINNING;
      retrieved_data.read((char *) &tmp, sizeof(int64_t));
      if (retrieved_data.good()) {
        LOG(INFO) << "start(OFFSET_STORED), starting from offset: " << tmp;
        last_commited_offset_ = tmp;
        last_flushed_offset_ = tmp;
        return tmp;
      }
    }
    LOG(INFO) << "start(OFFSET_STORED), read failed: starting from OFFSET_BEGINNING";
    return kspp::OFFSET_BEGINNING;
  }

  void s3_offset_storage::persist_offset(int64_t offset, int timeout_ms) {
    if (last_flushed_offset_ == last_commited_offset_)
      return;

    Aws::S3::Model::PutObjectRequest object_request;
    object_request.SetBucket(Aws::String(s3_bucket_.c_str()));
    object_request.SetKey(Aws::String(s3_object_name_.c_str()));

    auto data = Aws::MakeShared<Aws::StringStream>("PutObjectInputStream",
                                                   std::stringstream::in | std::stringstream::out |
                                                   std::stringstream::binary);
    data->write(reinterpret_cast<char *>(&last_commited_offset_), sizeof(int64_t));
    object_request.SetBody(data);

    // TODO I have no idea why I need a boost::interprocess::bufferstream - copied from internet and dopublecheck if is really needed...
    //std::shared_ptr<Aws::IOStream> body = std::shared_ptr<Aws::IOStream>(new boost::interprocess::bufferstream((char *) &_last_commited_offset, sizeof(int64_t)));
    //object_request.SetBody(body);

    LOG(INFO) << "begin persist_offset " << offset;
    auto put_object_outcome = s3_client_->PutObject(object_request);
    if (!put_object_outcome.IsSuccess()) {
      auto error = put_object_outcome.GetError();
      LOG(ERROR) << error.GetExceptionName() << ": " << error.GetMessage();
      return;
    }
    LOG(INFO) << "persist_offset done";
    last_flushed_offset_ = last_commited_offset_;
  }
}