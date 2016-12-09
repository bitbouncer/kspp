#include <assert.h>
#include <memory>
#include <functional>
#include <sstream>
#include "kspp_defs.h"
#include "kafka_producer.h"
#pragma once

namespace csi {
template<class K, class V, class codec>
class kafka_sink : public ksink<K, V>
{
  public:
  //typedef std::function<uint32_t(const K& key, const V& val)> partitioner;
  typedef std::function<uint32_t(const K& key)> partitioner;

  kafka_sink(std::string brokers, std::string topic, std::shared_ptr<codec> codec, partitioner p) : 
    _impl(brokers, topic), 
    _codec(codec), 
    _partitioner(p) 
  {}

  virtual ~kafka_sink() { 
    close(); 
  }
 
  virtual int produce(std::unique_ptr<krecord<K, V>> r) {
    void* kp = NULL;
    void* vp = NULL;
    size_t ksize = 0;
    size_t vsize = 0;

    std::stringstream ks;
    ksize = _codec->encode(r->key, ks);
    kp = malloc(ksize);
    ks.read((char*) kp, ksize);

    if (r->value) {
        std::stringstream vs;
        vsize = _codec->encode(*r->value, vs);
        vp = malloc(vsize);
        vs.read((char*) vp, vsize);
    }
    return _impl.produce(_partitioner(r->key), kafka_producer::FREE, kp, ksize, vp, vsize);
  }

  virtual void close() { 
    return _impl.close(); 
  }

  virtual size_t queue_len() { 
    return _impl.queue_len(); 
  }
  
  virtual std::string topic() const { 
    return _impl.topic(); 
  }
  
  virtual void poll(int timeout) { 
    return _impl.poll(timeout); 
  }
      
  private:
  kafka_producer          _impl;
  std::shared_ptr<codec>  _codec;
  partitioner             _partitioner;
};
};