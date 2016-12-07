#pragma once
#include <memory>
#include  <librdkafka/rdkafkacpp.h>
#include <boost/filesystem.hpp>
#include "kspp_defs.h"
#pragma once

namespace csi {
  class kafka_consumer
  {
  public:
    kafka_consumer(std::string brokers, std::string topic, int32_t partition);
    ~kafka_consumer();
    void close();
    std::unique_ptr<RdKafka::Message> consume();
    inline bool eof() const {
      return _eof;
    }
    std::string topic() const { return _topic; }

    void start(int64_t offset);

  private:
    const std::string  _topic;
    const int32_t      _partition;
    RdKafka::Topic*    _rd_topic;
    RdKafka::Consumer* _consumer;

    uint64_t _msg_cnt;
    uint64_t _msg_bytes;
    bool     _eof;
  };



  template<class K, class V, class codec>
  class kafka_source
  {
  public:

    kafka_source(std::string nodeid, std::string brokers, std::string topic, int32_t partition, std::string root_path, std::shared_ptr<codec> codec) :
      _codec(codec),
      _consumer(brokers, topic, partition) {}

    ~kafka_source() {
      close();
    }

    void start(int64_t offset) {
      _consumer.start(offset);
    }

    void close() {
      _consumer.close();
      //flush_offset();
    }

    inline bool eof() const {
      return _consumer.eof();
    }

    std::unique_ptr<krecord<K, V>> consume() {
      auto p = _consumer.consume();
      return p ? parse(p) : NULL;
    }


  private:
    std::unique_ptr<krecord<K, V>> parse(const std::unique_ptr<RdKafka::Message> & ref) {
      if (!ref)
        return NULL;

      std::unique_ptr<krecord<K, V>> res(new krecord<K, V>);
      
      res->event_time = ref->timestamp().timestamp;
      res->offset = ref->offset();
      {
        std::istrstream ks((const char*)ref->key_pointer(), ref->key_len());
        if (_codec->decode(ks, res->key) == 0)
        {
          std::cerr << "ksource::parse, decode key failed" << std::endl;
          return NULL;
        }
      }

      size_t sz = ref->len();
      if (sz)
      {
        std::istrstream vs((const char*)ref->payload(), sz);
        res->value = std::unique_ptr<V>(new V);
        size_t consumed = _codec->decode(vs, *res->value);
        if (consumed == 0)
        {
          std::cerr << "ksource::parse, decode value failed" << std::endl;
          return NULL;
        }
        assert(consumed == sz);
      }
      return res;
    }
    kafka_consumer          _consumer;
    std::shared_ptr<codec>  _codec;
  };
};

