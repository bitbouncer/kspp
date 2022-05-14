#include <kspp/state_stores/mem_token_bucket_store.h>
#include <chrono>

#pragma once

// this should be a template on storage type 
// ie mem_bucket or rocksdb version
// or virtual ptr to storage to be passed in constructor
// right now this is processing time rate limiting 
// how do we swap betweeen processing and event time??? TBD
namespace kspp {
  template<class K, class V>
  class thoughput_limiter : public event_consumer<K, V>, public partition_source<K, V> {
    static constexpr const char *PROCESSOR_NAME = "thoughput_limiter";
  public:
    thoughput_limiter(std::shared_ptr<cluster_config> config, std::shared_ptr<partition_source<K, V>> source,
                      double messages_per_sec)
        : event_consumer<K, V>(), partition_source<K, V>(source.get(), source->partition()), source_(source),
          token_bucket_(std::make_shared<mem_token_bucket_store<int, size_t>>
                            (std::chrono::milliseconds((
                                                           int) (1000.0 / messages_per_sec)), 1)) {
      source_->add_sink([this](auto r) {
        this->queue_.push_back(r);
      });
      this->add_metrics_label(KSPP_PROCESSOR_TYPE_TAG, "thoughput_limiter");
      this->add_metrics_label(KSPP_PARTITION_TAG, std::to_string(source->partition()));
    }

    ~thoughput_limiter() {
      close();
    }

    std::string log_name() const override {
      return PROCESSOR_NAME;
    }

    void start(int64_t offset) override {
      source_->start(offset);
      if (offset == kspp::OFFSET_BEGINNING)
        token_bucket_->clear();
    }

    void close() override {
      source_->close();
    }

    size_t process(int64_t tick) override {
      source_->process(tick);

      size_t processed = 0;
      while (this->queue_.next_event_time() <= tick) {
        auto trans = this->queue_.front();
        if (token_bucket_->consume(0, tick)) {
          this->lag_.add_event_time(tick, trans->event_time());
          ++(this->processed_count_);
          ++processed;
          this->queue_.pop_front();
          this->send_to_sinks(trans);
        } else {
          break;
        }
      }
      return processed;
    }

    void commit(bool flush) override {
      source_->commit(flush);
    }

    bool eof() const override {
      return (source_->eof() && (queue_size() == 0));
    }

    size_t queue_size() const override {
      return event_consumer<K, V>::queue_size();
    }

    int64_t next_event_time() const override {
      return event_consumer<K, V>::next_event_time();
    }

  private:
    std::shared_ptr<partition_source<K, V>> source_;
    std::shared_ptr<mem_token_bucket_store<int, size_t>> token_bucket_;
  };
} // namespace