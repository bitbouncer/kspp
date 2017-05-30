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

 

  virtual std::string name() const {
    return "kafka_topic_sink(" + _impl.topic() + ")-codec(" + CODEC::name() + ")[" + type_name<K>::get() + ", " + type_name<V>::get() + "]";
  }

  virtual std::string processor_name() const { return "kafka_sink(" + _impl.topic() + ")"; }

  virtual void close() {
    flush();
    return _impl.close();
  }

  virtual size_t queue_len() {
    return this->_queue.size() + _impl.queue_len();
  }

  virtual void poll(int timeout) {
    return _impl.poll(timeout);
  }

  virtual void commit(bool flush) {
    // noop
  }

  virtual void flush() {
    while (!eof()) {
      process_one(kspp::milliseconds_since_epoch());
      poll(0);
    }
    while (true) {
      auto ec = _impl.flush(1000);
      if (ec == 0)
        break;
    }
  }

  virtual bool eof() const {
    return this->_queue.size()==0; 
  }

  protected:
  kafka_sink_base(std::string brokers, std::string topic, std::chrono::milliseconds max_buffering_time, partitioner p, std::shared_ptr<CODEC> codec)
    : topic_sink<K, V>()
    , _codec(codec)
    , _impl(brokers, topic, max_buffering_time)
    , _partitioner(p)
    , _in_count("in_count")
    , _lag() {
    this->add_metric(&_in_count);
    this->add_metric(&_lag);
  }

  kafka_sink_base(std::string brokers, std::string topic, std::chrono::milliseconds max_buffering_time, std::shared_ptr<CODEC> codec)
    : topic_sink<K, V>()
    , _codec(codec)
    , _impl(brokers, topic, max_buffering_time)
    , _in_count("in_count")
    , _lag() {
    this->add_metric(&_in_count);
    this->add_metric(&_lag);
  }

  inline std::shared_ptr<CODEC> codec() {
    return _codec;
  }

  std::shared_ptr<CODEC> _codec;
  kafka_producer         _impl;
  partitioner            _partitioner;
  metric_counter         _in_count;
  metric_lag             _lag;
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

  virtual ~kafka_topic_sink() {
    this->close();
  }

  // lets try to get as much as possible from queue to librdkafka - stop when queue is empty or librdkafka fails
  virtual bool process_one(int64_t tick) {
    if (this->_queue.size() == 0)
      return false;
    size_t count = 0;

    while (this->_queue.size()) {
      auto ev = this->_queue.front();
      uint32_t partition_hash = 0;
      if (ev->_partition_hash < 0)
        partition_hash = (this->_partitioner) ? this->_partitioner(ev->record()->key) : kspp::get_partition_hash(ev->record()->key, this->codec());
      else
        partition_hash = (uint32_t) ev->_partition_hash;

      void* kp = nullptr;
      void* vp = nullptr;
      size_t ksize = 0;
      size_t vsize = 0;

      std::stringstream ks;
      ksize = this->_codec->encode(ev->record()->key, ks);
      kp = malloc(ksize);  // must match the free in kafka_producer TBD change to new[] and a memory pool
      ks.read((char*) kp, ksize);

      if (ev->record()->value) {
        std::stringstream vs;
        vsize = this->_codec->encode(*ev->record()->value, vs);
        vp = malloc(vsize);   // must match the free in kafka_producer TBD change to new[] and a memory pool
        vs.read((char*) vp, vsize);
      }
      if (this->_impl.produce(partition_hash, kafka_producer::FREE, kp, ksize, vp, vsize, ev->event_time(), ev->id()))
        return (count > 0);
      ++count;
      ++(this->_in_count);
      this->_lag.add_event_time(kspp::milliseconds_since_epoch(), ev->event_time()); // move outside loop
      this->_queue.pop_front();
    }
    return true;
  }

  protected:
    /*
    virtual int _produce(std::shared_ptr<kevent<K, V>> ev) {
      if (this->_partitioner)
        return _produce(this->_partitioner(ev->record()->key), ev);
      else
        return _produce(kspp::get_partition_hash(ev->record()->key, this->codec()), ev);
    }

    virtual int _produce(uint32_t partition, std::shared_ptr<kevent<K, V>> ev) {
      void* kp = nullptr;
      void* vp = nullptr;
      size_t ksize = 0;
      size_t vsize = 0;

      std::stringstream ks;
      ksize = this->_codec->encode(ev->record()->key, ks);
      kp = malloc(ksize);  // must match the free in kafka_producer TBD change to new[] and a memory pool
      ks.read((char*) kp, ksize);

      if (ev->record()->value) {
        std::stringstream vs;
        vsize = this->_codec->encode(*ev->record()->value, vs);
        vp = malloc(vsize);   // must match the free in kafka_producer TBD change to new[] and a memory pool
        vs.read((char*) vp, vsize);
      }
      ++(this->_in_count);
      this->_lag.add_event_time(kspp::milliseconds_since_epoch(), ev->event_time());
      return this->_impl.produce(partition, kafka_producer::FREE, kp, ksize, vp, vsize, ev->event_time(), ev->id());
    }
    */
};

//<null, VALUE>
template<class V, class CODEC>
class kafka_topic_sink<void, V, CODEC> : public kafka_sink_base<void, V, CODEC>
{
  public:
  kafka_topic_sink(topology_base& topology, std::string topic, std::shared_ptr<CODEC> codec = std::make_shared<CODEC>())
    : kafka_sink_base<void, V, CODEC>(topology.brokers(), topic, topology.max_buffering_time(), codec) {
  }

  virtual ~kafka_topic_sink() {
    this->close();
  }
  
  // lets try to get as much as possible from queue to librdkafka - stop when queue is empty or librdkafka fails
  virtual bool process_one(int64_t tick) {
    static uint32_t s_partition = 0;

    if (this->_queue.size() == 0)
      return false;

    size_t count = 0;

    while (this->_queue.size()) {
      auto ev = this->_queue.front();
      uint32_t partition_hash = (ev->_partition_hash < 0) ? ++s_partition : (uint32_t) ev->_partition_hash;
      void* vp = nullptr;
      size_t vsize = 0;

      if (ev->record()->value) {
        std::stringstream vs;
        vsize = this->_codec->encode(*ev->record()->value, vs);
        vp = malloc(vsize);   // must match the free in kafka_producer TBD change to new[] and a memory pool
        vs.read((char*) vp, vsize);
      }
      if (this->_impl.produce(partition_hash, kafka_producer::FREE, nullptr, 0, vp, vsize, ev->event_time(), ev->id()))
        return (count > 0);
      ++count;
      ++(this->_in_count);
      this->_lag.add_event_time(kspp::milliseconds_since_epoch(), ev->event_time()); // move outside loop
      this->_queue.pop_front();
    }
    return true;
  }


  protected:
  //  virtual int _produce(std::shared_ptr<kevent<void, V>> ev) {
  //    static uint32_t partition = 0;
  //    // it does not matter that this is not thread safe since we really does not care where the message goes
  //    return _produce(++partition, ev);
  //  }
  //
  //  virtual int _produce(uint32_t partition, std::shared_ptr<kevent<void, V>> ev) {
  //    void* vp = nullptr;
  //    size_t vsize = 0;
  //
  //    if (ev->record()->value) {
  //      std::stringstream vs;
  //      vsize = this->_codec->encode(*ev->record()->value, vs);
  //      vp = malloc(vsize);  // must match the free in kafka_producer TBD change to new[] and a memory pool
  //      vs.read((char*) vp, vsize);
  //    }
  //    ++(this->_in_count);
  //    this->_lag.add_event_time(kspp::milliseconds_since_epoch(), ev->event_time());
  //    return this->_impl.produce(partition, kafka_producer::FREE, nullptr, 0, vp, vsize, ev->event_time(), ev->id());
  //  }
  //};
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

    virtual ~kafka_topic_sink() {
      this->close();
    }

    // lets try to get as much as possible from queue to librdkafka - stop when queue is empty or librdkafka fails
    virtual bool process_one(int64_t tick) {
      if (this->_queue.size() == 0)
        return false;
      size_t count = 0;

      while (this->_queue.size()) {
        auto ev = this->_queue.front();
        uint32_t partition_hash = 0;
        if (ev->_partition_hash < 0)
          partition_hash = (this->_partitioner) ? this->_partitioner(ev->record()->key) : kspp::get_partition_hash(ev->record()->key, this->codec());
        else
          partition_hash = (uint32_t) ev->_partition_hash;

        void* kp = nullptr;
        //void* vp = nullptr;
        size_t ksize = 0;
        //size_t vsize = 0;

        std::stringstream ks;
        ksize = this->_codec->encode(ev->record()->key, ks);
        kp = malloc(ksize);  // must match the free in kafka_producer TBD change to new[] and a memory pool
        ks.read((char*) kp, ksize);

        /*
        if (ev->record()->value) {
          std::stringstream vs;
          vsize = this->_codec->encode(*ev->record()->value, vs);
          vp = malloc(vsize);   // must match the free in kafka_producer TBD change to new[] and a memory pool
          vs.read((char*) vp, vsize);
        }
        */

        if (this->_impl.produce(partition_hash, kafka_producer::FREE, kp, ksize, nullptr, 0, ev->event_time(), ev->id()))
          return (count > 0);
        ++count;
        ++(this->_in_count);
        this->_lag.add_event_time(kspp::milliseconds_since_epoch(), ev->event_time()); // move outside loop
        this->_queue.pop_front();
      }
      return true;
    }


    protected:
      //virtual int _produce(std::shared_ptr<kevent<K, void>> ev) {
      //  void* kp = nullptr;
      //  size_t ksize = 0;

      //  std::stringstream ks;
      //  ksize = this->_codec->encode(ev->record()->key, ks);
      //  kp = malloc(ksize);  // must match the free in kafka_producer TBD change to new[] and a memory pool
      //  ks.read((char*) kp, ksize);

      //  if (this->_partitioner)
      //    return _produce(this->_partitioner(ev->record()->key), ev);
      //  else
      //    return _produce(kspp::get_partition_hash(ev->record()->key, this->codec()), ev);
      //}

      //virtual int _produce(uint32_t partition, std::shared_ptr<kevent<K, void>> ev) {
      //  void* kp = nullptr;
      //  size_t ksize = 0;

      //  std::stringstream ks;
      //  ksize = this->_codec->encode(ev->record()->key, ks);
      //  kp = malloc(ksize);  // must match the free in kafka_producer TBD change to new[] and a memory pool
      //  ks.read((char*) kp, ksize);
      //  ++(this->_in_count);
      //  this->_lag.add_event_time(kspp::milliseconds_since_epoch(), ev->event_time());
      //  return this->_impl.produce(partition, kafka_producer::FREE, kp, ksize, nullptr, 0, ev->event_time(), ev->id());
      //}
  };
};

