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

template<class K, class V, class CODEC>
class kafka_sink : public ksink<K, V>
{
  public:
  using partitioner = typename kafka_partitioner_base<K>::partitioner;

  kafka_sink(std::string brokers, std::string topic, partitioner p, std::shared_ptr<CODEC> codec = std::make_shared<CODEC>()) :
    _impl(brokers, topic), 
    _codec(codec), 
    _partitioner(p),
    _fixed_partition(-1)
  {}

  kafka_sink(std::string brokers, std::string topic, int32_t partition, std::shared_ptr<CODEC> codec = std::make_shared<CODEC>()) :
    _impl(brokers, topic),
    _codec(codec),
    _fixed_partition(partition) 
  {}
  
  virtual ~kafka_sink() { 
    close(); 
  }

  virtual std::string name() const {
    return "kafka_sink-" + _impl.topic();
  }

  virtual void close() {
    return _impl.close();
  }

  virtual size_t queue_len() {
    return _impl.queue_len();
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
  std::shared_ptr<CODEC>  _codec;
  partitioner             _partitioner;
  int32_t                 _fixed_partition;
};

//<null, VALUE>
template<class V, class CODEC>
class kafka_sink<void, V, CODEC> : public ksink<void, V>
{
  public:
  kafka_sink(std::string brokers, std::string topic, int32_t partition, std::shared_ptr<CODEC> codec = std::make_shared<CODEC>()) :
    _impl(brokers, topic),
    _codec(codec),
    _fixed_partition(partition) {}

  virtual ~kafka_sink() {
    close();
  }

  virtual std::string name() const {
    return _impl.topic() + "-kafka_sink";
  }

  virtual void close() {
    return _impl.close();
  }

  virtual size_t queue_len() {
    return _impl.queue_len();
  }

  /*
  virtual std::string topic() const {
    return _impl.topic();
  }
  */

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
  std::shared_ptr<CODEC>  _codec;
  int32_t                 _fixed_partition;
};

// <key, NULL>
template<class K, class CODEC>
class kafka_sink<K, void, CODEC> : public ksink<K, void>
{
public:
  using partitioner = typename kafka_partitioner_base<K>::partitioner;

  kafka_sink(std::string brokers, std::string topic, partitioner p, std::shared_ptr<CODEC> codec = std::make_shared<CODEC>()) :
    _impl(brokers, topic),
    _codec(codec),
    _partitioner(p),
    _fixed_partition(-1) {}

  kafka_sink(std::string brokers, std::string topic, int32_t partition, std::shared_ptr<CODEC> codec = std::make_shared<CODEC>()) :
    _impl(brokers, topic),
    _codec(codec),
    _fixed_partition(partition) {}

  virtual ~kafka_sink() {
    close();
  }

  virtual std::string name() const {
    return "kafka_sink-" + _impl.topic();
  }

  virtual void close() {
    return _impl.close();
  }

  virtual size_t queue_len() {
    return _impl.queue_len();
  }


  virtual void poll(int timeout) {
    return _impl.poll(timeout);
  }

  virtual int produce(std::shared_ptr<krecord<K, void>> r) {
    void* kp = NULL;
    size_t ksize = 0;

    std::stringstream ks;
    ksize = _codec->encode(r->key, ks);
    kp = malloc(ksize);
    ks.read((char*)kp, ksize);

    if (_partitioner)
      return _impl.produce(_partitioner(r->key), kafka_producer::FREE, kp, ksize, NULL, 0);
    else
      return _impl.produce(_fixed_partition, kafka_producer::FREE, kp, ksize, NULL, 0);
  }

private:
  kafka_producer          _impl;
  std::shared_ptr<CODEC>  _codec;
  partitioner             _partitioner;
  int32_t                 _fixed_partition;
};

};