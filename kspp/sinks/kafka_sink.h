#include <assert.h>
#include <memory>
#include <functional>
#include <sstream>
#include <kspp/kspp.h>
#include <kspp/impl/sinks/kafka_producer.h>
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
class kafka_sink_base : public topic_sink<K, V>
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
  virtual bool process_one(int64_t tick) {
    // noop
    return false;
  }

  virtual void commit(bool flush) {
    // noop
  }

  virtual void flush() {
    while (true) {
      auto ec = _impl.flush(1000);
      if (ec == 0)
        break;
    }
  }

  virtual bool eof() const {
    //return (_impl.queue_len() == 0);
    return true;
  }

  protected:
  kafka_sink_base(std::string brokers, std::string topic, std::chrono::milliseconds max_buffering_time, partitioner p, std::shared_ptr<CODEC> codec)
    : topic_sink<K, V>()
    , _codec(codec)
    , _impl(brokers, topic, max_buffering_time)
    , _partitioner(p)
    , _in_count("in_count") {
    this->add_metric(&_in_count);
  }

  kafka_sink_base(std::string brokers, std::string topic, std::chrono::milliseconds max_buffering_time, std::shared_ptr<CODEC> codec)
    : topic_sink<K, V>()
    , _codec(codec)
    , _impl(brokers, topic, max_buffering_time)
    , _in_count("in_count") {
    this->add_metric(&_in_count);
  }
  
  inline std::shared_ptr<CODEC> codec() {
    return _codec;
  }

  std::shared_ptr<CODEC> _codec;
  kafka_producer         _impl;
  partitioner            _partitioner;
  metric_counter         _in_count;
};

template<class K, class V, class CODEC>
class kafka_topic_sink : public kafka_sink_base<K, V, CODEC>
{
  public:
  enum { MAX_KEY_SIZE = 1000 };

  using partitioner = typename kafka_partitioner_base<K>::partitioner;

  kafka_topic_sink(topology_base& topology, std::string topic, partitioner p, std::shared_ptr<CODEC> codec = std::make_shared<CODEC>())
    : kafka_sink_base<K, V, CODEC>(topology.brokers(), topic, topology.max_buffering_time(), p, codec) {
  }

  kafka_topic_sink(topology_base& topology, std::string topic, std::shared_ptr<CODEC> codec = std::make_shared<CODEC>())
    : kafka_sink_base<K, V, CODEC>(topology.brokers(), topic, topology.max_buffering_time(), codec) {
  }
   
protected:
  virtual int _produce(std::shared_ptr<ktransaction<K, V>> transaction) {
    if (this->_partitioner)
      return _produce(this->_partitioner(transaction->record()->key), transaction);
    else
      return _produce(kspp::get_partition_hash(transaction->record()->key, this->codec()), transaction);
  }

  virtual int _produce(uint32_t partition, std::shared_ptr<ktransaction<K, V>> transaction) {
    void* kp = nullptr;
    void* vp = nullptr;
    size_t ksize = 0;
    size_t vsize = 0;

    std::stringstream ks;
    ksize = this->_codec->encode(transaction->record()->key, ks);
    kp = malloc(ksize);  // must match the free in kafka_producer TBD change to new[] and a memory pool
    ks.read((char*) kp, ksize);

    if (transaction->record()->value) {
      std::stringstream vs;
      vsize = this->_codec->encode(*transaction->record()->value, vs);
      vp = malloc(vsize);   // must match the free in kafka_producer TBD change to new[] and a memory pool
      vs.read((char*) vp, vsize);
    }
    ++(this->_in_count);
    return this->_impl.produce(partition, kafka_producer::FREE, kp, ksize, vp, vsize, transaction->event_time(), transaction->id());
  }
};

//<null, VALUE>
template<class V, class CODEC>
class kafka_topic_sink<void, V, CODEC> : public kafka_sink_base<void, V, CODEC>
{
  public:
  kafka_topic_sink(topology_base& topology, std::string topic, std::shared_ptr<CODEC> codec = std::make_shared<CODEC>())
    : kafka_sink_base<void, V, CODEC>(topology.brokers(), topic, topology.max_buffering_time(), codec) {
  }

protected:
  virtual int _produce(std::shared_ptr<ktransaction<void, V>> transaction) {
    static uint32_t partition = 0;
    // it does not matter that this is not thread safe since we really does not care where the message goes
    return _produce(++partition, transaction);
  }

  virtual int _produce(uint32_t partition, std::shared_ptr<ktransaction<void, V>> transaction) {
    void* vp = nullptr;
    size_t vsize = 0;

    if (transaction->record()->value) {
      std::stringstream vs;
      vsize = this->_codec->encode(*transaction->record()->value, vs);
      vp = malloc(vsize);  // must match the free in kafka_producer TBD change to new[] and a memory pool
      vs.read((char*) vp, vsize);
    }
    ++(this->_in_count);
    return this->_impl.produce(partition, kafka_producer::FREE, nullptr, 0, vp, vsize, transaction->event_time(), transaction->id());
  }
};

// <key, nullptr>
template<class K, class CODEC>
class kafka_topic_sink<K, void, CODEC> : public kafka_sink_base<K, void, CODEC>
{
  public:
  using partitioner = typename kafka_partitioner_base<K>::partitioner;

  kafka_topic_sink(topology_base& topology, std::string topic, partitioner p, std::shared_ptr<CODEC> codec = std::make_shared<CODEC>())
    : kafka_sink_base<K, void, CODEC>(topology.brokers(), topic, topology.max_buffering_time(), p, codec) {
  }

  kafka_topic_sink(topology_base& topology, std::string topic, std::shared_ptr<CODEC> codec = std::make_shared<CODEC>())
    : kafka_sink_base<K, void, CODEC>(topology.brokers(), topic, topology.max_buffering_time(), codec) {
  }

protected:
  virtual int _produce(std::shared_ptr<ktransaction<K, void>> transaction) {
    void* kp = nullptr;
    size_t ksize = 0;

    std::stringstream ks;
    ksize = this->_codec->encode(transaction->record()->key, ks);
    kp = malloc(ksize);  // must match the free in kafka_producer TBD change to new[] and a memory pool
    ks.read((char*) kp, ksize);

    if (this->_partitioner)
      return _produce(this->_partitioner(transaction->record()->key), transaction);
    else
      return _produce(kspp::get_partition_hash(transaction->record()->key, this->codec()), transaction);
  }

  virtual int _produce(uint32_t partition, std::shared_ptr<ktransaction<K, void>> transaction) {
    void* kp = nullptr;
    size_t ksize = 0;

    std::stringstream ks;
    ksize = this->_codec->encode(transaction->record()->key, ks);
    kp = malloc(ksize);  // must match the free in kafka_producer TBD change to new[] and a memory pool
    ks.read((char*) kp, ksize);
    ++(this->_in_count);
    return this->_impl.produce(partition, kafka_producer::FREE, kp, ksize, nullptr, 0, transaction->event_time(), transaction->id());
  }
};
};

