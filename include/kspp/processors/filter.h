#include <kspp/kspp.h>
#pragma once
namespace kspp {
  template<class K, class V>
  class filter : public event_consumer<K, V>, public partition_source<K, V> {
    static constexpr const char* PROCESSOR_NAME = "filter";
  public:
    typedef std::function<bool(std::shared_ptr<const krecord <K, V>> record)> predicate; // return true to keep

    filter(topology &t, std::shared_ptr<partition_source < K, V>> source, predicate f)
    : event_consumer<K, V>()
    , partition_source<K, V>(source.get(), source->partition())
    , _source(source)
    , _predicate(f)
    , _predicate_false("predicate_false", "msg") {
      _source->add_sink([this](auto r) {
        this->_queue.push_back(r);
      });
      this->add_metric(&_predicate_false);
      this->add_metrics_tag(KSPP_PROCESSOR_TYPE_TAG, "filter");
      this->add_metrics_tag(KSPP_PARTITION_TAG, std::to_string(source->partition()));
    }

    ~filter() {
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
      size_t processed = 0;

      while (this->_queue.next_event_time()<=tick){
        auto trans = this->_queue.pop_and_get();
        ++processed;
       this->_lag.add_event_time(tick, trans->event_time());
        ++(this->_processed_count);
        if (_predicate(trans->record())) {
          this->send_to_sinks(trans);
        } else {
          ++_predicate_false;
        }
      }
      return processed;
    }

    void commit(bool flush) override {
      _source->commit(flush);
    }

    bool eof() const override {
      return (queue_size() == 0) && _source->eof();
    }

    size_t queue_size() const override {
      return event_consumer<K, V > ::queue_size();
    }

    int64_t next_event_time() const override {
      return event_consumer<K, V>::next_event_time();
    }


  private:
    std::shared_ptr<partition_source < K, V>> _source;
    predicate _predicate;
    metric_counter _predicate_false;
  };
} // namespace