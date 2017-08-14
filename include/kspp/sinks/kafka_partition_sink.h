#include <assert.h>
#include <memory>
#include <functional>
#include <sstream>
#include <kspp/kspp.h>
#include <kspp/impl/sinks/kafka_producer.h>

#pragma once

namespace kspp {
  // SINGLE PARTITION PRODUCER
  // this is just to only override the necessary key value specifications
  template<class K, class V, class CODEC>
  class kafka_partition_sink_base : public partition_sink<K, V> {
  protected:
    kafka_partition_sink_base(std::string brokers, std::string topic, int32_t partition,
                              std::chrono::milliseconds max_buffering_time, std::shared_ptr<CODEC> codec)
            : partition_sink<K, V>(partition), _codec(codec), _impl(brokers, topic, max_buffering_time),
              _fixed_partition(partition), _in_count("in_count"), _lag() {
      this->add_metric(&_in_count);
      this->add_metric(&_lag);
    }

    virtual std::string simple_name() const {
      return "kafka_partition_sink(" + _impl.topic() + ")";
    }

    void close() override {
      flush();
      return _impl.close();
    }

    size_t queue_len() const override {
      return event_consumer<K, V>::queue_len() + _impl.queue_len();
    }

    void poll(int timeout) override {
      return _impl.poll(timeout);
    }

    void commit(bool flush) override {
      // noop
    }

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

    bool process_one(int64_t tick) override {
      size_t count = 0;
      while (this->_queue.size()) {
        auto ev = this->_queue.front();
        if (handle_event(ev) != 0)
          return (count > 0);
        ++count;
        ++(this->_in_count);
        this->_lag.add_event_time(kspp::milliseconds_since_epoch(), ev->event_time()); // move outside loop
        this->_queue.pop_front();
      }
      return (count > 0);
    }

  protected:
    virtual int handle_event(std::shared_ptr<kevent < K, V>>) = 0;

    kafka_producer _impl;
    std::shared_ptr<CODEC> _codec;
    size_t _fixed_partition;
    metric_counter _in_count;
    metric_lag _lag;
  };

  template<class K, class V, class CODEC>
  class kafka_partition_sink : public kafka_partition_sink_base<K, V, CODEC> {
  public:
    kafka_partition_sink(topology_base &topology, int32_t partition, std::string topic,
                         std::shared_ptr<CODEC> codec = std::make_shared<CODEC>())
            : kafka_partition_sink_base<K, V, CODEC>(topology.brokers(), topic, partition,
                                                     topology.max_buffering_time(), codec) {
    }

    ~kafka_partition_sink() override {
      this->close();
    }

  protected:
    int handle_event(std::shared_ptr<kevent < K, V>> ev) override {
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
      return this->_impl.produce((uint32_t) this->_fixed_partition, kafka_producer::FREE, kp, ksize, vp, vsize,
                                 ev->event_time(), ev->id());
    }
  };

// value only topic
  template<class V, class CODEC>
  class kafka_partition_sink<void, V, CODEC> : public kafka_partition_sink_base<void, V, CODEC> {
  public:
    kafka_partition_sink(topology_base &topology, int32_t partition, std::string topic,
                         std::shared_ptr<CODEC> codec = std::make_shared<CODEC>())
            : kafka_partition_sink_base<void, V, CODEC>(topology.brokers(), topic, partition,
                                                        topology.max_buffering_time(), codec) {
    }

    ~kafka_partition_sink() override {
      this->close();
    }

  protected:
    int handle_event(std::shared_ptr<kevent < void, V>> ev) override {
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
      return this->_impl.produce((uint32_t) this->_fixed_partition, kafka_producer::FREE, nullptr, 0, vp, vsize,
                                 ev->event_time(), ev->id());
    }
  };

// key only topic
  template<class K, class CODEC>
  class kafka_partition_sink<K, void, CODEC> : public kafka_partition_sink_base<K, void, CODEC> {
  public:
    kafka_partition_sink(topology_base &topology, int32_t partition, std::string topic,
                         std::shared_ptr<CODEC> codec = std::make_shared<CODEC>())
            : kafka_partition_sink_base<K, void, CODEC>(topology.brokers(), topic, partition,
                                                        topology.max_buffering_time(), codec) {
    }

    ~kafka_partition_sink() override {
      this->close();
    }

  protected:
    int handle_event(std::shared_ptr<kevent < K, void>> ev) override {
      void *kp = nullptr;
      size_t ksize = 0;
      std::stringstream ks;
      ksize = this->_codec->encode(ev->record()->key(), ks);
      kp = malloc(ksize);  // must match the free in kafka_producer TBD change to new[] and a memory pool
      ks.read((char *) kp, ksize);
      return this->_impl.produce((uint32_t) this->_fixed_partition, kafka_producer::FREE, kp, ksize, nullptr, 0,
                                 ev->event_time(), ev->id());
    }
  };
};

