#include <memory>
#include <kspp/kspp.h>
#pragma once

namespace kspp {
  template<class K, class V>
  class null_sink : public topic_sink<K, V> {
  public:
    null_sink(topology &t)
    : topic_sink<K, V>() {
    }

    ~null_sink() override {
      this->flush();;
    }

    void close() override {
    }

    std::string simple_name() const override {
      return "null_sink";
    }

    size_t queue_size() const override {
      return event_consumer<K, V>::queue_size();
    }

    void flush() override {
      while (process_one(0))
      {
        ; // noop
      }
    }

    bool eof() const override {
      return this->_queue.size();
    }

    bool process_one(int64_t tick) override {
      size_t count = 0;
      while (this->_queue.size()) {
        auto ev = this->_queue.front();
        this->_queue.pop_front();
      }
      return (count > 0);
    }
  };
}