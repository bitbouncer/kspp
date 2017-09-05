#include <memory>
#include <kspp/kspp.h>
#pragma once

namespace kspp {
  template<class K, class V>
  class null_sink : public partition_sink<K, V> {
  public:
    null_sink(topology &t, std::shared_ptr<partition_source<K, V>> source)
    : partition_sink<K, V>(source->partition()) {
      source->add_sink(this);
    }

    ~null_sink() override {
      this->flush();;
    }

    std::string simple_name() const override {
      return "null_sink";
    }

    size_t queue_len() const override {
      return event_consumer<K, V>::queue_len();
    }

    void commit(bool flush) override {
      // noop
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