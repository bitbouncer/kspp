#include <kspp/kspp.h>
#pragma once
namespace kspp {
  template<class K, class V>
  class delay : public event_consumer<K, V>, public partition_source<K, V> {
    static constexpr const char* PROCESSOR_NAME = "delay";
  public:
    typedef std::function<bool(std::shared_ptr < kevent < K, V >> record)> predicate; // return true to keep

    delay(std::shared_ptr<cluster_config> config, std::shared_ptr <partition_source<K, V>> source, std::chrono::milliseconds delaytime)
        : event_consumer<K, V>()
        , partition_source<K, V>(source.get(), source->partition())
        , source_(source)
        , delay_(delaytime.count()) {
      source_->add_sink([this](auto r) { this->_queue.push_back(r); });
      this->add_metrics_label(KSPP_PROCESSOR_TYPE_TAG, "delay");
      this->add_metrics_label(KSPP_PARTITION_TAG, std::to_string(source->partition()));
    }

    ~delay() {
      close();
    }

    std::string log_name() const override {
      return PROCESSOR_NAME;
    }

    void start(int64_t offset) override {
      source_->start(offset);
    }

    void close() override {
      source_->close();
    }

    size_t process(int64_t tick) override {
      source_->process(tick);

      size_t processed=0;
      while (this->_queue.next_event_time()<=tick) {
        auto r = this->_queue.front();
        if (r->event_time() + delay_ <= tick) {
          this->_lag.add_event_time(tick, r->event_time());
          ++(this->_processed_count);
          this->_queue.pop_front();
          this->send_to_sinks(r);
          ++processed;
        } else {
          break;
        }
      }
      return processed;
    }

    void commit(bool flush) override {
      source_->commit(flush);
    }

    size_t queue_size() const override {
      return this->_queue.size();
    }

    int64_t next_event_time() const override {
      return event_consumer<K, V>::next_event_time();
    }

    bool eof() const override  {
      return ((queue_size() == 0) && source_->eof());
    }

  private:
    std::shared_ptr <partition_source<K, V>> source_;
    int delay_;
  };
} // namespace