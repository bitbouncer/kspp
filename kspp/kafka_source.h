#include <memory>
#include <boost/filesystem.hpp>
#include "kspp_defs.h"
#include "kafka_consumer.h"
#pragma once

namespace csi {
template<class K, class V, class codec>
class kafka_source : public ksource<K, V>
{
  public:
  //kafka_source(std::string nodeid, std::string brokers, std::string topic, int32_t partition, std::string root_path, std::shared_ptr<codec> codec) :
  kafka_source(std::string brokers, std::string topic, int32_t partition, std::shared_ptr<codec> codec) :
    _codec(codec),
    _consumer(brokers, topic, partition) {}

  virtual ~kafka_source() {
    close();
  }

  virtual void start(int64_t offset) {
    _consumer.start(offset);
  }

  //TBD get offsets from kafka
  virtual void start() {
    _consumer.start(-2);
  }

  virtual void close() {
    _consumer.close();
  }

  virtual inline bool eof() const {
    return _consumer.eof();
  }

  // TBD lazy commit offsets to kafka
  virtual void commit() {
  }
  
  // TBD hard commit offsets to kafka
  virtual void flush_offset() {
  }
  
  virtual  std::unique_ptr<krecord<K, V>> consume() {
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
      std::istrstream ks((const char*) ref->key_pointer(), ref->key_len());
      if (_codec->decode(ks, res->key) == 0) {
        std::cerr << "ksource::parse, topic: " << _consumer.topic() << ", decode key failed, actual key sz:" << ref->key_len() << std::endl;
        return NULL;
      }
    }

    size_t sz = ref->len();
    if (sz) {
      std::istrstream vs((const char*) ref->payload(), sz);
      res->value = std::unique_ptr<V>(new V);
      size_t consumed = _codec->decode(vs, *res->value);
      if (consumed == 0) {
        std::cerr << "ksource::parse, topic: " << _consumer.topic() << ", decode value failed, actual payload sz:" << sz <<std::endl;
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

