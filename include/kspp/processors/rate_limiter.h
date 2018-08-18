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
  class rate_limiter : public event_consumer<K, V>, public partition_source<K, V> {
    static constexpr const char* PROCESSOR_NAME = "rate_limiter";
  public:
    rate_limiter(std::shared_ptr<cluster_config> config, std::shared_ptr<partition_source<K, V>> source,
                 std::chrono::milliseconds agetime, size_t capacity)
            : event_consumer<K, V>(), partition_source<K, V>(source.get(), source->partition())
        , _source(source)
        , _token_bucket(std::make_shared<mem_token_bucket_store<K, size_t>>(agetime, capacity))
        , _rejection_count("rejection_count", "msg") {
      this->add_metrics_tag(KSPP_PROCESSOR_TYPE_TAG, "rate_limiter");
      this->add_metrics_tag(KSPP_PARTITION_TAG, std::to_string(source->partition()));
      _source->add_sink([this](auto r) {
        this->_queue.push_back(r);
      });
      this->add_metric(&_rejection_count);
    }

    ~rate_limiter() {
      close();
    }

    std::string log_name() const override {
      return PROCESSOR_NAME;
    }

    void start(int64_t offset) override {
      _source->start(offset);
      if (offset == kspp::OFFSET_BEGINNING)
        _token_bucket->clear();
    }

    void close() override {
      _source->close();
    }

    size_t process(int64_t tick) override {
      _source->process(tick);
      size_t processed = 0;
      while (this->_queue.next_event_time()<=tick) {
        auto trans = this->_queue.pop_and_get();
        ++processed;
        ++(this->_processed_count);
        this->_lag.add_event_time(tick, trans->event_time());
        // milliseconds_since_epoch for processing time limiter
        //
        if (_token_bucket->consume(trans->record()->key(), trans->event_time())) { // TBD tick???
          this->send_to_sinks(trans);
        } else {
          ++_rejection_count;
        }
      }
      return processed;
    }

    void commit(bool flush) override {
      _source->commit(flush);
    }

    bool eof() const override {
      return _source->eof() && (queue_size() == 0);
    }

    size_t queue_size() const override {
      return event_consumer<K, V>::queue_size();
    }

    int64_t next_event_time() const override {
      return event_consumer<K, V>::next_event_time();
    }


  private:
    std::shared_ptr<partition_source<K, V>> _source;
    std::shared_ptr<mem_token_bucket_store<K, size_t>> _token_bucket;
    metric_counter _rejection_count;
  };
} // namespace