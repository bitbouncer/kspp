#include <memory>
#include <kspp/kspp.h>
#pragma once

namespace kspp {
  template<class K, class V>
  class genric_topic_sink : public topic_sink<K, V> {
  public:
    typedef std::function<void(std::shared_ptr<const krecord <K, V>> record)> handler;

    genric_topic_sink(topology &t, handler f)
        : topic_sink<K, V>()
        , _handler(f) {
    }

    ~genric_topic_sink() override {
      this->flush();
    }

    void close() override {
    }

    std::string simple_name() const override {
      return "genric_topic_sink";
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
        _handler(r->record());
        ++processed;
      }

      return processed;
    }

  protected:
    handler _handler;
  };
}