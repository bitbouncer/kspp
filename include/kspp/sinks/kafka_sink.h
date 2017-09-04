#include <assert.h>
#include <memory>
#include <functional>
#include <sstream>
#include <kspp/kspp.h>
#include <kspp/impl/sinks/kafka_producer.h>

#pragma once

namespace kspp {
  template<class K>
  class kafka_partitioner_base {
  public:
    using partitioner = typename std::function<uint32_t(const K &key)>;
  };

  template<>
  class kafka_partitioner_base<void> {
  public:
    using partitioner = typename std::function<uint32_t(void)>;
  };

  template<class K, class V, class CODEC>
  class kafka_sink_base : public topic_sink<K, V> {
  public:
    enum { MAX_KEY_SIZE = 1000 };

    using partitioner = typename kafka_partitioner_base<K>::partitioner;

    std::string simple_name() const override {
      return "kafka_sink(" + _impl.topic() + ")";
    }

    void close() override {
      flush();
      return _impl.close();
    }

    size_t queue_len() const override {
      return topic_sink<K, V>::queue_len() + _impl.queue_len();
    }

    void poll(int timeout) override {
      return _impl.poll(timeout);
    }

    /*
     * void commit(bool flush) override {
      // noop
    }
     */

    void flush() override {
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

    bool eof() const override {
      return this->_queue.size() == 0;
    }

    // lets try to get as much as possible from queue to librdkafka - stop when queue is empty or librdkafka fails
    bool process_one(int64_t tick) override {
      size_t count = 0;
      while (this->_queue.size()) {
        auto ev = this->_queue.front();
        int ec = handle_event(ev);
        if (ec == 0) {
          ++count;
          ++(this->_in_count);
          this->_lag.add_event_time(kspp::milliseconds_since_epoch(), ev->event_time()); // move outside loop
          this->_queue.pop_front();
          continue;
        }

        if (ec == RdKafka::ERR__QUEUE_FULL) {
          // expected and retriable
          return (count > 0);
        }
        // permanent failure - need to stop TBD
        return (count > 0);
      } // while
      return (count > 0);
    }

  protected:
    kafka_sink_base(std::shared_ptr<cluster_config> cconfig,
                    std::string topic,
                    partitioner p,
                    std::shared_ptr<CODEC> codec)
            : topic_sink<K, V>()
        , _codec(codec)
        , _impl(cconfig, topic)
        , _partitioner(p)
        , _in_count("in_count")
        , _lag() {
      this->add_metric(&_in_count);
      this->add_metric(&_lag);
    }

    kafka_sink_base(std::shared_ptr<cluster_config> cconfig,
                    std::string topic,
                    std::shared_ptr<CODEC> codec)
            : topic_sink<K, V>()
        , _codec(codec)
        , _impl(cconfig, topic)
        , _in_count("in_count")
        , _lag() {
      this->add_metric(&_in_count);
      this->add_metric(&_lag);
    }

    virtual int handle_event(std::shared_ptr<kevent<K, V>>) = 0;

    inline std::shared_ptr<CODEC> codec() {
      return _codec;
    }

    std::shared_ptr<CODEC> _codec;
    kafka_producer _impl;
    partitioner _partitioner;
    metric_counter _in_count;
    metric_lag _lag;
  };

  template<class K, class V, class CODEC>
  class kafka_sink : public kafka_sink_base<K, V, CODEC> {
  public:
    enum { MAX_KEY_SIZE = 1000 };

    using partitioner = typename kafka_partitioner_base<K>::partitioner;

    kafka_sink(topology &topology, std::string topic, partitioner p,
                     std::shared_ptr<CODEC> codec = std::make_shared<CODEC>())
            : kafka_sink_base<K, V, CODEC>(topology.get_cluster_config(), topic, p, codec) {
    }

    kafka_sink(topology &topology, std::string topic,
                     std::shared_ptr<CODEC> codec = std::make_shared<CODEC>())
            : kafka_sink_base<K, V, CODEC>(topology.get_cluster_config(), topic, codec) {
    }

    ~kafka_sink() override {
      this->close();
    }

  protected:
    int handle_event(std::shared_ptr<kevent<K, V>> ev) override {
      uint32_t partition_hash = 0;

      if (ev->has_partition_hash())
        partition_hash = ev->partition_hash();
      else
        partition_hash = (this->_partitioner) ? this->_partitioner(ev->record()->key()) : kspp::get_partition_hash(
                ev->record()->key(), this->codec());

      void *kp = nullptr;
      void *vp = nullptr;
      size_t ksize = 0;
      size_t vsize = 0;

      std::stringstream ks;
      ksize = this->_codec->encode(ev->record()->key(), ks);
      kp = malloc(ksize);  // must match the free in kafka_producer TBD change to new[] and a memory pool
      ks.read((char *) kp, ksize);

      if (ev->record()->value()) {
        std::stringstream vs;
        vsize = this->_codec->encode(*ev->record()->value(), vs);
        vp = malloc(vsize);   // must match the free in kafka_producer TBD change to new[] and a memory pool
        vs.read((char *) vp, vsize);
      }
      return this->_impl.produce(partition_hash, kafka_producer::FREE, kp, ksize, vp, vsize, ev->event_time(),
                                 ev->id());
    }
  };

//<null, VALUE>
  template<class V, class CODEC>
  class kafka_sink<void, V, CODEC> : public kafka_sink_base<void, V, CODEC> {
  public:
    kafka_sink(topology &topology, std::string topic,
                     std::shared_ptr<CODEC> codec = std::make_shared<CODEC>())
            : kafka_sink_base<void, V, CODEC>(topology.get_cluster_config(), topic, codec) {
    }

    ~kafka_sink() override {
      this->close();
    }

  protected:
    int handle_event(std::shared_ptr<kevent<void, V>> ev) override {
      static uint32_t s_partition = 0;
      uint32_t partition_hash = ev->has_partition_hash() ? ev->partition_hash() : ++s_partition;
      void *vp = nullptr;
      size_t vsize = 0;

      if (ev->record()->value()) {
        std::stringstream vs;
        vsize = this->_codec->encode(*ev->record()->value(), vs);
        vp = malloc(vsize);   // must match the free in kafka_producer TBD change to new[] and a memory pool
        vs.read((char *) vp, vsize);
      } else {
        assert(false);
        return 0; // no writing of null key and null values
      }
      return this->_impl.produce(partition_hash, kafka_producer::FREE, nullptr, 0, vp, vsize, ev->event_time(),
                                 ev->id());
    }
  };

  // <key, nullptr>
  template<class K, class CODEC>
  class kafka_sink<K, void, CODEC> : public kafka_sink_base<K, void, CODEC> {
  public:
    using partitioner = typename kafka_partitioner_base<K>::partitioner;

    kafka_sink(topology &topology, std::string topic, partitioner p,
                     std::shared_ptr<CODEC> codec = std::make_shared<CODEC>())
            : kafka_sink_base<K, void, CODEC>(topology.get_cluster_config(), topic, p, codec) {
    }

    kafka_sink(topology &topology, std::string topic,
                     std::shared_ptr<CODEC> codec = std::make_shared<CODEC>())
            : kafka_sink_base<K, void, CODEC>(topology.get_cluster_config(), topic, codec) {
    }

    ~kafka_sink() override {
      this->close();
    }

  protected:
    int handle_event(std::shared_ptr<kevent<K, void>> ev) override {
      uint32_t partition_hash = 0;
      if (ev->has_partition_hash())
        partition_hash = ev->partition_hash();
      else
        partition_hash = (this->_partitioner) ? this->_partitioner(ev->record()->key()) : kspp::get_partition_hash(
                ev->record()->key(), this->codec());

      void *kp = nullptr;
      size_t ksize = 0;

      std::stringstream ks;
      ksize = this->_codec->encode(ev->record()->key(), ks);
      kp = malloc(ksize);  // must match the free in kafka_producer TBD change to new[] and a memory pool
      ks.read((char *) kp, ksize);

      return this->_impl.produce(partition_hash, kafka_producer::FREE, kp, ksize, nullptr, 0, ev->event_time(),
                                 ev->id());
    }
  };
}

