#include <memory>
#include <kspp/kspp.h>
#pragma once

namespace kspp {
  template<class K, class V>
  class genric_topic_sink : public topic_sink<K, V> {
    static constexpr const char* PROCESSOR_NAME = "genric_sink";
  public:
    typedef std::function<void(std::shared_ptr<const krecord <K, V>> record)> handler;

    genric_topic_sink(std::shared_ptr<cluster_config> config, handler f)
        : topic_sink<K, V>()
        , _handler(f) {
      this->add_metrics_tag(KSPP_PROCESSOR_TYPE_TAG, "genric_sink");
    }

    ~genric_topic_sink() override {
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
        auto r = this->_queue.pop_and_get();
        this->_lag.add_event_time(tick, r->event_time());
        ++(this->_processed_count);
        _handler(r->record());
        ++processed;
      }

      return processed;
    }

  protected:
    handler _handler;
  };
}