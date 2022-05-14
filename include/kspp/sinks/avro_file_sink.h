#include <memory>
#include <avro/Generic.hh>
#include <avro/DataFile.hh>
#include <kspp/avro/avro_utils.h>
#include <kspp/kspp.h>

#pragma once

namespace kspp {

  template<class V>
  class avro_file_sink : public topic_sink<void, V> {
    static constexpr const char *PROCESSOR_NAME = "avro_file_sink";
  public:
    avro_file_sink(std::shared_ptr<cluster_config> config, std::string parent_path, std::string base_name,
                   std::chrono::seconds window_size)
        : topic_sink<void, V>(), parent_path_(parent_path), base_name_(base_name), window_size_(window_size) {
      this->add_metrics_label(KSPP_PROCESSOR_TYPE_TAG, PROCESSOR_NAME);
    }

    ~avro_file_sink() override {
      this->flush();
      this->close();
    }

    void close() override {
      close_file();
      LOG(INFO) << PROCESSOR_NAME << " processor closed - consumed " << this->processed_count_.value() << " messages";
    }

    void close_file() {
      if (file_writer_) {
        file_writer_->flush();
        file_writer_->close();
        file_writer_.reset();
        LOG(INFO) << PROCESSOR_NAME << ", file: " << current_file_name_ << " closed - written " << messages_in_file_
                  << " messages";
      }
    }

    std::string log_name() const override {
      return PROCESSOR_NAME;
    }

    size_t queue_size() const override {
      return event_consumer<void, V>::queue_size();
    }

    void flush() override {
      while (process(kspp::milliseconds_since_epoch()) > 0) { ; // noop
      }
    }

    bool eof() const override {
      return this->queue_.size();
    }

    size_t process(int64_t tick) override {
      size_t processed = 0;

      //forward up this timestamp
      while (this->queue_.next_event_time() <= tick) {
        auto r = this->queue_.pop_front_and_get();
        this->lag_.add_event_time(tick, r->event_time());

        // check if it's time to rotate
        if (file_writer_ && r->event_time() >= end_of_windows_ts_) {
          close_file();
        }

        // time to create a new file?
        if (!file_writer_) {
          //auto schema = r->record()->value()->valid_schema();
          auto schema = avro_utils::avro_utils<V>::valid_schema(*r->record()->value());
          current_file_name_ = parent_path_ + "/" + base_name_ + "-" + std::to_string(r->event_time()) + ".avro";
          end_of_windows_ts_ = r->event_time() + (window_size_.count() *
                                                  1000); // should we make another kind of window that plays nice with 24h?
          messages_in_file_ = 0;
#ifdef SNAPPY_CODEC_AVAILABLE
          file_writer_ = std::make_shared<avro::DataFileWriter<V>>(current_file_name_.c_str(), *schema, 16 * 1024,
                                                                   avro::SNAPPY_CODEC);
#else
          _file_writer = std::make_shared<avro::DataFileWriter<V>>(_current_file_name.c_str(), *schema, 16 * 1024, avro::DEFLATE_CODEC);
#endif
        }

        // null value protection
        if (r->record()->value()) {
          ++messages_in_file_;
          //_file_writer->write(*r->record()->value()->generic_datum());
          file_writer_->write(*r->record()->value());
        }
        ++(this->processed_count_);
        ++processed;
      }
      return processed;
    }

  protected:
    std::shared_ptr<avro::DataFileWriter<V>> file_writer_;
    std::string parent_path_;
    std::string base_name_;
    std::string current_file_name_;
    const std::chrono::seconds window_size_;
    int64_t end_of_windows_ts_ = 0;
    int64_t messages_in_file_ = 0;
  };
}