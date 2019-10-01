#include <memory>
#include <kspp/kspp.h>
#pragma once

/**
 *  array_topic_sink - useful for testing
 *  stores the records in an std::vector given in constructor
 */

namespace kspp {
  template<class K, class V>
  class array_topic_sink : public topic_sink<K, V> {
    static constexpr const char* PROCESSOR_NAME = "array_sink";
  public:
    array_topic_sink(std::shared_ptr<cluster_config> config, std::vector<std::shared_ptr<const krecord <K, V>>>* a)
        : topic_sink<K, V>()
        , _array(a) {
      this->add_metrics_label(KSPP_PROCESSOR_TYPE_TAG, "array_sink");
    }

    ~array_topic_sink() override {
      this->flush();
    }

    void close() override {
    }

    std::string log_name() const override {
      return PROCESSOR_NAME;
    }

    size_t queue_size() const override {
      return event_consumer<K, V>::queue_size();
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
        ++(this->_processed_count);
        _array->push_back((r->record()));
        ++processed;
      }

      return processed;
    }

  protected:
    std::vector<std::shared_ptr<const krecord <K, V>>>* _array;
  };
}
