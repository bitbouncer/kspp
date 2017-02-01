#include <assert.h>
#include <memory>
#include <functional>
#include <sstream>
#include <kspp/kspp.h>
#include "kafka_producer.h"
#pragma once

namespace kspp {
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
class kafka_sink_base : public topic_sink<K, V, CODEC>
{
  public:
  enum { MAX_KEY_SIZE = 1000 };

  using partitioner = typename kafka_partitioner_base<K>::partitioner;

  virtual ~kafka_sink_base() {
    close();
  }

  virtual std::string name() const {
    return "kafka_topic_sink(" + _impl.topic() + ")-codec(" + CODEC::name() + ")[" + type_name<K>::get() + ", " + type_name<V>::get() + "]";
  }

  virtual std::string processor_name() const { return "kafka_sink(" + _impl.topic() +")"; }

  virtual void close() {
    return _impl.close();
  }

  virtual size_t queue_len() {
    return _impl.queue_len();
  }

  virtual void poll(int timeout) {
    return _impl.poll(timeout);
  }

  // pure sink cannot suck data from upstream...
  virtual bool process_one() {
    return false;
  }

  // need_wait as interface??
  // in_queue_size() as interface size grow -> process more
  // out_queue_size() as interface -> size grows sleep more...

  virtual bool eof() const {
    return true;
    //return _impl.queue_len() > 0; # this will cause 100% cpu when we think there is something to process - when we really need to wait...    
  }

  protected:
  kafka_sink_base(std::string brokers, std::string topic, partitioner p, std::shared_ptr<CODEC> codec)
    : topic_sink<K, V, CODEC>(codec)
    , _impl(brokers, topic)
    , _partitioner(p)
    , _count("messages") {
    this->add_metric(&_count);
  }

  kafka_sink_base(std::string brokers, std::string topic, std::shared_ptr<CODEC> codec)
    : topic_sink<K, V, CODEC>(codec)
    , _impl(brokers, topic)
    , _count("messages") {
    this->add_metric(&_count);
  }

  kafka_producer _impl;
  partitioner    _partitioner;
  metric_counter _count;
};

template<class K, class V, class CODEC>
class kafka_topic_sink : public kafka_sink_base<K, V, CODEC>
{
  public:
  enum { MAX_KEY_SIZE = 1000 };

  using partitioner = typename kafka_partitioner_base<K>::partitioner;

  kafka_topic_sink(topology_base& topology, std::string topic, partitioner p, std::shared_ptr<CODEC> codec)
    : kafka_sink_base<K, V, CODEC>(topology.brokers(), topic, p, codec) {
  }

  kafka_topic_sink(topology_base& topology, std::string topic, std::shared_ptr<CODEC> codec)
    : kafka_sink_base<K, V, CODEC>(topology.brokers(), topic, codec) {
  }

  virtual int produce(std::shared_ptr<krecord<K, V>> r) {
    if (this->_partitioner)
      return produce(this->_partitioner(r->key), r);
    else
      return produce(get_partition_hash(r->key, this->codec()), r);
  }

  virtual int produce(uint32_t partition, std::shared_ptr<krecord<K, V>> r) {
    void* kp = nullptr;
    void* vp = nullptr;
    size_t ksize = 0;
    size_t vsize = 0;

    std::stringstream ks;
    ksize = this->_codec->encode(r->key, ks);
    kp = malloc(ksize);
    ks.read((char*) kp, ksize);

    if (r->value) {
      std::stringstream vs;
      vsize = this->_codec->encode(*r->value, vs);
      vp = malloc(vsize);
      vs.read((char*) vp, vsize);
    }
    ++(this->_count);
    return this->_impl.produce(partition, kafka_producer::FREE, kp, ksize, vp, vsize);
  }
};

//<null, VALUE>
template<class V, class CODEC>
class kafka_topic_sink<void, V, CODEC> : public kafka_sink_base<void, V, CODEC>
{
  public:
  kafka_topic_sink(topology_base& topology, std::string topic, std::shared_ptr<CODEC> codec)
    : kafka_sink_base<void, V, CODEC>(topology.brokers(), topic, codec) {
  }

  virtual int produce(uint32_t partition, std::shared_ptr<krecord<void, V>> r) {
    void* vp = nullptr;
    size_t vsize = 0;

    if (r->value) {
      std::stringstream vs;
      vsize = this->_codec->encode(*r->value, vs);
      vp = malloc(vsize);
      vs.read((char*) vp, vsize);
    }
    ++(this->_count);
    return this->_impl.produce(partition, kafka_producer::FREE, nullptr, 0, vp, vsize);
  }
};

// <key, nullptr>
template<class K, class CODEC>
class kafka_topic_sink<K, void, CODEC> : public kafka_sink_base<K, void, CODEC>
{
  public:
  using partitioner = typename kafka_partitioner_base<K>::partitioner;

  kafka_topic_sink(topology_base& topology, std::string topic, partitioner p, std::shared_ptr<CODEC> codec)
    : kafka_sink_base<K, void, CODEC>(topology.brokers(), topic, p, codec) {
  }

  kafka_topic_sink(topology_base& topology, std::string topic, std::shared_ptr<CODEC> codec)
    : kafka_sink_base<K, void, CODEC>(topology.brokers(), topic, codec) {
  }

  virtual int produce(std::shared_ptr<krecord<K, void>> r) {
    void* kp = nullptr;
    size_t ksize = 0;

    std::stringstream ks;
    ksize = this->_codec->encode(r->key, ks);
    kp = malloc(ksize);
    ks.read((char*) kp, ksize);

    if (this->_partitioner)
      return produce(this->_partitioner(r->key), r);
    else
      return produce(get_partition_hash(r->key, this->codec()), r);
  }

  virtual int produce(uint32_t partition, std::shared_ptr<krecord<K, void>> r) {
    void* kp = nullptr;
    size_t ksize = 0;

    std::stringstream ks;
    ksize = this->_codec->encode(r->key, ks);
    kp = malloc(ksize);
    ks.read((char*) kp, ksize);
    ++(this->_count);
    return this->_impl.produce(partition, kafka_producer::FREE, kp, ksize, nullptr, 0);
  }
};

// SINGLE PARTITION PRODUCER
// this is just to only override the necessary key value specifications
template<class K, class V, class CODEC>
class kafka_partition_sink_base : public partition_sink<K, V>
{
  protected:
  kafka_partition_sink_base(std::string brokers, std::string topic, size_t partition, std::shared_ptr<CODEC> codec)
    : partition_sink<K, V>(partition)
    , _codec(codec)
    , _impl(brokers, topic)
    , _fixed_partition(partition)
    , _count("count") {
    this->add_metric(&_count);
  }

  virtual ~kafka_partition_sink_base() {
    close();
  }

  virtual std::string name() const {
    return "kafka_partition_sink(" + _impl.topic() + "#" + std::to_string(_fixed_partition) + ")-codec(" + CODEC::name() + ")[" + type_name<K>::get() + ", " + type_name<V>::get() + "]";
  }

  virtual std::string processor_name() const { return "kafka_partition_sink(" + _impl.topic() + ")"; }

  virtual void close() {
    return _impl.close();
  }

  virtual size_t queue_len() {
    return _impl.queue_len();
  }

  virtual void poll(int timeout) {
    return _impl.poll(timeout);
  }

  // pure sink cannot suck data from upstream...
  virtual bool process_one() {
    return false;
  }

  virtual bool eof() const {
    return true;
    //return _impl.queue_len() > 0; # this will cause 100% cpu when we think there is something to process - when we really need to wait...
  }

  protected:
  kafka_producer          _impl;
  std::shared_ptr<CODEC>  _codec;
  size_t                  _fixed_partition;
  metric_counter          _count;
};

template<class K, class V, class CODEC>
class kafka_partition_sink : public kafka_partition_sink_base<K, V, CODEC>
{
  public:
  kafka_partition_sink(topology_base& topology, std::string topic, std::shared_ptr<CODEC> codec)
    : kafka_partition_sink_base<K, V, CODEC>(topology.brokers(), topic, topology.partition(), codec) {
  }

  virtual int produce(std::shared_ptr<krecord<K, V>> r) {
    void* kp = nullptr;
    void* vp = nullptr;
    size_t ksize = 0;
    size_t vsize = 0;

    std::stringstream ks;
    ksize = this->_codec->encode(r->key, ks);
    kp = malloc(ksize);
    ks.read((char*) kp, ksize);

    if (r->value) {
      std::stringstream vs;
      vsize = this->_codec->encode(*r->value, vs);
      vp = malloc(vsize);
      vs.read((char*) vp, vsize);
    }
    ++(this->_count);
    return this->_impl.produce((uint32_t) this->_fixed_partition, kafka_producer::FREE, kp, ksize, vp, vsize);
  }
};

// value only topic
template<class V, class CODEC>
class kafka_partition_sink<void, V, CODEC> : public kafka_partition_sink_base<void, V, CODEC>
{
  public:
  kafka_partition_sink(topology_base& topology, std::string topic, std::shared_ptr<CODEC> codec)
    : kafka_partition_sink_base<void, V, CODEC>(topology.brokers(), topic, topology.partition(), codec) {
  }
 
  virtual int produce(std::shared_ptr<krecord<void, V>> r) {
    void* vp = nullptr;
    size_t vsize = 0;

    if (r->value) {
      std::stringstream vs;
      vsize = this->_codec->encode(*r->value, vs);
      vp = malloc(vsize);
      vs.read((char*) vp, vsize);
    }
    ++(this->_count);
    return this->_impl.produce((uint32_t) this->_fixed_partition, kafka_producer::FREE, nullptr, 0, vp, vsize);
  }
};

// key only topic
template<class K, class CODEC>
class kafka_partition_sink<K, void, CODEC> : public kafka_partition_sink_base<K, void, CODEC>
{
  public:
  kafka_partition_sink(topology_base& topology, std::string topic, std::shared_ptr<CODEC> codec)
    : kafka_partition_sink_base<K, void, CODEC>(topology.brokers(), topic, topology.partition(), codec) {
  }

  virtual int produce(std::shared_ptr<krecord<K, void>> r) {
    void* kp = nullptr;
    size_t ksize = 0;

    std::stringstream ks;
    ksize = this->_codec->encode(r->key, ks);
    kp = malloc(ksize);
    ks.read((char*) kp, ksize);
    ++(this->_count);
    return this->_impl.produce((uint32_t) this->_fixed_partition, kafka_producer::FREE, kp, ksize, nullptr, 0);
  }
};
};

