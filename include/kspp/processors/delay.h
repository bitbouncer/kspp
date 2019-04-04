#include <kspp/kspp.h>
#pragma once
namespace kspp {
  template<class K, class V>
  class delay : public partition_source<K, V> {
    static constexpr const char* PROCESSOR_NAME = "delay";
  public:
    typedef std::function<bool(std::shared_ptr < kevent < K, V >> record)> predicate; // return true to keep

    delay(std::shared_ptr<cluster_config> config, std::shared_ptr <partition_source<K, V>> source, int ms)
            : partition_source<K, V>(source->partition()), _source(source), _delay(ms) {
      _source->add_sink([this](auto r) { this->_queue.push_back(r); });
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
      _source->start(offset);
    }

    void close() override {
      _source->close();
    }

    size_t process(int64_t tick) override {
      _source->process(tick);

      size_t processed=0;
      while (this->_queue.next_event_time()<=tick) {
        auto r = this->_queue.front();
        if (r->event_time + _delay > tick) {
          _lag.add_event_time(tick, r->event_time());
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

    void commit(flush) override {
      _source->commit(flush);
    }

    size_t queue_size() const override {
      return this->_queue.size();
    }

    int64_t next_event_time() const override {
      return event_consumer<K, V>::next_event_time();
    }

    bool eof() const override  {
      return (queue_size() == 0) && _source->eof());
    }

  private:
    std::shared_ptr <partition_source<K, V>> _source;
    int _delay;
  };
} // namespace