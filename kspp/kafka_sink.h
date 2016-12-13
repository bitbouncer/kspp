#include <assert.h>
#include <memory>
#include <functional>
#include <sstream>
#include "kspp_defs.h"
#include "kafka_producer.h"
#pragma once

namespace csi {

template<class K>
class kafka_partitioner_base
{
  public:
  using partitioner = typename std::function<uint32_t(const K& key)>;
};

template<>
class kafka_partitioner_base<void>
{
  public:
  using partitioner = typename std::function<uint32_t(void)>;
};

template<class K, class V, class codec>
class kafka_sink : public ksink<K, V>
{
  public:
  using partitioner = typename kafka_partitioner_base<K>::partitioner;
  
   /*
  using partitioner = typename std::conditional< 
    std::is_void<K>::value, 
    std::function<uint32_t()>, 
    std::function<uint32_t(const K& key)> 
  >::type;
  */

  kafka_sink(std::string brokers, std::string topic, std::shared_ptr<codec> codec, partitioner p) : 
    _impl(brokers, topic), 
    _codec(codec), 
    _partitioner(p),
    _fixed_partition(-1)
  {}

  kafka_sink(std::string brokers, std::string topic, std::shared_ptr<codec> codec, int32_t partition) :
    _impl(brokers, topic),
    _codec(codec),
    _fixed_partition(partition) 
  {}
  
  virtual ~kafka_sink() { 
    close(); 
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

  virtual int produce(std::shared_ptr<krecord<K, V>> r) {
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
    if (_partitioner)
      return _impl.produce(_partitioner(r->key), kafka_producer::FREE, kp, ksize, vp, vsize);
    else
      return _impl.produce(_fixed_partition, kafka_producer::FREE, kp, ksize, vp, vsize);
  }

      
  private:
  kafka_producer          _impl;
  std::shared_ptr<codec>  _codec;
  partitioner             _partitioner;
  int32_t                 _fixed_partition;
};


template<class V, class codec>
class kafka_sink<void, V, codec> : public ksink<void, V>
{
  public:
  kafka_sink(std::string brokers, std::string topic, std::shared_ptr<codec> codec, int32_t partition) :
    _impl(brokers, topic),
    _codec(codec),
    _fixed_partition(partition) {}

  virtual ~kafka_sink() {
    close();
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

  virtual int produce(std::shared_ptr<krecord<void, V>> r) {
    void* vp = NULL;
    size_t vsize = 0;

    if (r->value) {
      std::stringstream vs;
      vsize = _codec->encode(*r->value, vs);
      vp = malloc(vsize);
      vs.read((char*) vp, vsize);
    }
    return _impl.produce(_fixed_partition, kafka_producer::FREE, NULL, 0, vp, vsize);
  }
  private:
  kafka_producer          _impl;
  std::shared_ptr<codec>  _codec;
  int32_t                 _fixed_partition;
};
};