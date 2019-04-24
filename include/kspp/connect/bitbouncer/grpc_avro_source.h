#include <memory>
#include <strstream>
#include <thread>
#include <glog/logging.h>
#include <kspp/kspp.h>
#include <kspp/topology.h>
#include "grpc_avro_consumer.h"
#pragma once

namespace kspp {
  template<class K, class V>
  class grpc_avro_source : public partition_source<K, V> {
    static constexpr const char *PROCESSOR_NAME = "bb_grpc_avro_source";
  public:
    grpc_avro_source(std::shared_ptr<cluster_config> config,
                          int32_t partition,
                          std::string topic,
                          std::string offset_storage_path,
                          std::string uri,
                          std::string api_key,
                          std::string secret_access_key)
        : partition_source<K, V>(nullptr, partition)
        , _impl(partition, topic, offset_storage_path, uri, api_key, secret_access_key) {
    }

    virtual ~grpc_avro_source() {
      close();
    }

    std::string log_name() const override {
      return PROCESSOR_NAME;
    }

    void start(int64_t offset) override {
      _impl.start(offset);
    }

    void close() override {
      /*
       * if (_commit_chain.last_good_offset() >= 0 && _impl.commited() < _commit_chain.last_good_offset())
        _impl.commit(_commit_chain.last_good_offset(), true);
        */
      _impl.close();
    }

    bool eof() const override {
      return _impl.eof();
    }

    void commit(bool flush) override {
      _impl.commit(flush);
    }

    inline int64_t offset() const {
      return _impl.offset();
    }

    inline bool good() const {
      return _impl.good();
    }

    // TBD if we store last offset and end of stream offset we can use this...
    size_t queue_size() const override {
      return _impl.queue().size();
    }

    int64_t next_event_time() const override {
      return _impl.queue().next_event_time();
    }

    size_t process(int64_t tick) override {
      if (_impl.queue().size() == 0)
        return 0;
      size_t processed = 0;
      while (!_impl.queue().empty()) {
        auto p = _impl.queue().front();
        if (p == nullptr || p->event_time() > tick)
          return processed;
        _impl.queue().pop_front();
        this->send_to_sinks(p);
        ++(this->_processed_count);
        ++processed;
        this->_lag.add_event_time(tick, p->event_time());
      }
      return processed;
    }

    std::string topic() const override {
      return _impl.logical_name();
    }

  protected:
    grpc_avro_consumer<K, V> _impl;
  };
}

