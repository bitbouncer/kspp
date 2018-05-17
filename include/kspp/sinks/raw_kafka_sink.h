#include <assert.h>
#include <memory>
#include <functional>
#include <sstream>
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

  template<class K, class V, class KEY_CODEC, class VAL_CODEC>
  class raw_kafka_sink_base {
    static constexpr const char* PROCESSOR_NAME = "kafka_sink";
  public:
    virtual ~raw_kafka_sink_base() {}

    enum { MAX_KEY_SIZE = 1000 };

    using partitioner = typename kafka_partitioner_base<K>::partitioner;

    std::string log_name() const {
      return PROCESSOR_NAME;
    }

    std::string topic() const {
      return _topic;
    }

    void close() {
      flush();
      return _impl.close();
    }

    size_t queue_size() const {
      return topic_sink<K, V>::queue_size() + _impl.queue_size();
    }

    void poll(int timeout) {
      return _impl.poll(timeout);
    }

    void flush() {
      while (!eof()) {
        process(kspp::milliseconds_since_epoch());
        poll(0);
      }

      while (true) {
        auto ec = _impl.flush(1000);
        if (ec == 0)
          break;
      }
    }

    bool eof() const {
      return this->_queue.size() == 0;
    }

    // lets try to get as much as possible from queue to librdkafka - stop when queue is empty or librdkafka fails
    size_t process(int64_t tick) {
      size_t count = 0;
      while (this->_queue.size()) {
        auto ev = this->_queue.front();
        int ec = handle_event(ev);
        if (ec == 0) {
          ++count;
          ++(this->_processed_count);
          this->_lag.add_event_time(kspp::milliseconds_since_epoch(), ev->event_time()); // move outside loop
          this->_queue.pop_front();
          continue;
        }

        if (ec == RdKafka::ERR__QUEUE_FULL) {
          // expected and retriable
          return count;
        }

        LOG(FATAL) << "RDKafa permanent failure - exiting ec:" << ec;
        // permanent failure - need to stop TBD
        return count;
      } // while
      return count;
    }

  protected:
    raw_kafka_sink_base(std::shared_ptr<cluster_config> config,
                        std::string topic,
                        partitioner p,
                        std::shared_ptr<KEY_CODEC> key_codec,
                        std::shared_ptr<VAL_CODEC> val_codec)
        : _topic(topic)
        , _key_codec(key_codec)
        ,  _val_codec(val_codec)
        , _key_schema_id(-1)
        , _val_schema_id(-1)
        , _impl(config, topic)
        , _partitioner(p)
        , _processed_count("processed", "msg")
        , _lag() {
      this->add_metric(&_processed_count);
      this->add_metric(&_lag);
    }

    raw_kafka_sink_base(std::shared_ptr<cluster_config> config,
                        std::string topic,
                        std::shared_ptr<KEY_CODEC> key_codec,
                        std::shared_ptr<VAL_CODEC> val_codec)
        : _topic(topic)
        , _key_codec(key_codec)
        , _val_codec(val_codec)
        , _key_schema_id(-1)
        , _val_schema_id(-1)
        , _impl(config, topic)
        , _processed_count("processed", "msg")
        , _lag() {
      this->add_metric(&_processed_count);
      this->add_metric(&_lag);
    }

    virtual int handle_event(std::shared_ptr<kevent<K, V>>) = 0;

    void add_metric(metric *p) {
      _metrics.push_back(p);
    }

    std::string _topic;
    std::vector<metric*> _metrics;
    kspp::event_queue<K, V> _queue;
    std::shared_ptr<KEY_CODEC> _key_codec;
    std::shared_ptr<VAL_CODEC> _val_codec;
    int32_t _key_schema_id;
    int32_t _val_schema_id;
    kafka_producer _impl;
    partitioner _partitioner;
    metric_counter _processed_count;
    metric_lag _lag;
  };

  template<class K, class V, class KEY_CODEC, class VAL_CODEC>
  class raw_kafka_sink : public raw_kafka_sink_base<K, V, KEY_CODEC, VAL_CODEC> {
  public:
    enum { MAX_KEY_SIZE = 1000 };

    using partitioner = typename kafka_partitioner_base<K>::partitioner;

    raw_kafka_sink(std::shared_ptr<cluster_config> config,
                   std::string topic,
                   partitioner p,
                   std::shared_ptr<KEY_CODEC> key_codec = std::make_shared<KEY_CODEC>(),
                   std::shared_ptr<VAL_CODEC> val_codec = std::make_shared<VAL_CODEC>())
        : raw_kafka_sink_base<K, V, KEY_CODEC, VAL_CODEC>(config, topic, p, key_codec, val_codec) {
    }

    raw_kafka_sink(std::shared_ptr<cluster_config> config,
                   std::string topic,
                   std::shared_ptr<KEY_CODEC> key_codec = std::make_shared<KEY_CODEC>(),
                   std::shared_ptr<VAL_CODEC> val_codec = std::make_shared<VAL_CODEC>())
        : raw_kafka_sink_base<K, V, KEY_CODEC, VAL_CODEC>(config, topic, key_codec, val_codec) {
    }

    ~raw_kafka_sink() override {
      this->close();
    }

    inline void produce(std::shared_ptr<const krecord<K, V>> r, std::function<void(int64_t offset, int32_t ec)> callback) {
      auto am = std::make_shared<commit_chain::autocommit_marker>(callback);
      this->_queue.push_back(std::make_shared<kevent<K, V>>(r, am));
    }

    inline void produce(uint32_t partition_hash, std::shared_ptr<const krecord<K, V>> r, std::function<void(int64_t offset, int32_t ec)> callback) {
      auto am = std::make_shared<commit_chain::autocommit_marker>(callback);
      this->_queue.push_back(std::make_shared<kevent<K, V>>(r, am, partition_hash));
    }

    inline void produce(const K &key, const V &value, int64_t ts, std::function<void(int64_t offset, int32_t ec)> callback) {
      produce(std::make_shared<const krecord<K, V>>(key, value, ts), callback);
    }

    inline void
    produce(uint32_t partition_hash, const K &key, const V &value, int64_t ts, std::function<void(int64_t offset, int32_t ec)> callback) {
      produce(partition_hash, std::make_shared<const krecord<K, V>>(key, value, ts), callback);
    }

  protected:
    int handle_event(std::shared_ptr<kevent<K, V>> ev) override {
      uint32_t partition_hash = 0;

      // first time??
      // register schemas under the topic-key, topic-value name to comply with kafka-connect behavior
      if (this->_key_schema_id<0) {
        this->_key_schema_id = this->_key_codec->register_schema(this->topic() + "-key", ev->record()->key());
        LOG_IF(FATAL, this->_key_schema_id<0) << "Failed to register schema - aborting";
      }

      if (this->_val_schema_id<0 && ev->record()->value()) {
        this->_val_schema_id = this->_val_codec->register_schema(this->topic() + "-value", *ev->record()->value());
        LOG_IF(FATAL, this->_val_schema_id<0) << "Failed to register schema - aborting";
      }



      if (ev->has_partition_hash())
        partition_hash = ev->partition_hash();
      else
        partition_hash = (this->_partitioner) ? this->_partitioner(ev->record()->key()) : kspp::get_partition_hash(
            ev->record()->key(), this->_key_codec);

      void *kp = nullptr;
      void *vp = nullptr;
      size_t ksize = 0;
      size_t vsize = 0;

      std::stringstream ks;
      ksize = this->_key_codec->encode(ev->record()->key(), ks);
      kp = malloc(ksize);  // must match the free in kafka_producer TBD change to new[] and a memory pool
      ks.read((char *) kp, ksize);

      if (ev->record()->value()) {
        std::stringstream vs;
        vsize = this->_val_codec->encode(*ev->record()->value(), vs);
        vp = malloc(vsize);   // must match the free in kafka_producer TBD change to new[] and a memory pool
        vs.read((char *) vp, vsize);
      }
      //return this->_impl.produce(partition_hash, raw_kafka_producer::FREE, kp, ksize, vp, vsize, ev->event_time(), ev->id());
      return this->_impl.produce(partition_hash, kafka_producer::FREE, kp, ksize, vp, vsize, ev->event_time(), ev->id());
    }
  };

//<null, VALUE>
  template<class V, class VAL_CODEC>
  class raw_kafka_sink<void, V, void, VAL_CODEC> : public raw_kafka_sink_base<void, V, void, VAL_CODEC> {
  public:
    raw_kafka_sink(std::shared_ptr<cluster_config> config,
                   std::string topic,
                   std::shared_ptr<VAL_CODEC> val_codec = std::make_shared<VAL_CODEC>())
        : raw_kafka_sink_base<void, V, void, VAL_CODEC>(config, topic, nullptr, val_codec) {
    }

    ~raw_kafka_sink() override {
      this->close();
    }

    inline void produce(std::shared_ptr<const krecord<void, V>> r, std::function<void(int64_t offset, int32_t ec)> callback) {
      auto am = std::make_shared<commit_chain::autocommit_marker>(callback);
      this->_queue.push_back(std::make_shared<kevent<void, V>>(r, am));
    }

    inline void produce(uint32_t partition_hash, std::shared_ptr<const krecord<void, V>> r, std::function<void(int64_t offset, int32_t ec)> callback) {
      auto am = std::make_shared<commit_chain::autocommit_marker>(callback);
      this->_queue.push_back(std::make_shared<kevent<void, V>>(r, am, partition_hash));
    }

    inline void produce(const V &value, int64_t ts, std::function<void(int64_t offset, int32_t ec)> callback) {
      produce(std::make_shared<const krecord<void, V>>(value, ts), callback);
    }

    inline void
    produce(uint32_t partition_hash, const V &value, int64_t ts, std::function<void(int64_t offset, int32_t ec)> callback) {
      produce(partition_hash, std::make_shared<const krecord<void, V>>(value, ts), callback);
    }

  protected:
    int handle_event(std::shared_ptr<kevent<void, V>> ev) override {
      static uint32_t s_partition = 0;
      uint32_t partition_hash = ev->has_partition_hash() ? ev->partition_hash() : ++s_partition;
      void *vp = nullptr;
      size_t vsize = 0;

      // first time??
      // register schemas under the topic-key, topic-value name to comply with kafka-connect behavior
      if (this->_val_schema_id<0 && ev->record()->value()) {
        this->_val_schema_id = this->_val_codec->register_schema(this->topic() + "-value", *ev->record()->value());
        LOG_IF(FATAL, this->_val_schema_id<0) << "Failed to register schema - aborting";
      }

      if (ev->record()->value()) {
        std::stringstream vs;
        vsize = this->_val_codec->encode(*ev->record()->value(), vs);
        vp = malloc(vsize);   // must match the free in kafka_producer TBD change to new[] and a memory pool
        vs.read((char *) vp, vsize);
      } else {
        assert(false);
        return 0; // no writing of null key and null values
      }
      return this->_impl.produce(partition_hash, kafka_producer::FREE, nullptr, 0, vp, vsize, ev->event_time(), ev->id());
    }
  };

  // <key, nullptr>
  template<class K, class KEY_CODEC>
  class raw_kafka_sink<K, void, KEY_CODEC, void> : public raw_kafka_sink_base<K, void, KEY_CODEC, void> {
  public:
    using partitioner = typename kafka_partitioner_base<K>::partitioner;

    raw_kafka_sink(std::shared_ptr<cluster_config> config,
                   std::string topic, partitioner p,
                   std::shared_ptr<KEY_CODEC> key_codec = std::make_shared<KEY_CODEC>())
        : raw_kafka_sink_base<K, void, KEY_CODEC, void>(config, topic, p, key_codec, nullptr) {
    }

    raw_kafka_sink(std::shared_ptr<cluster_config> config,
                   std::string topic,
                   std::shared_ptr<KEY_CODEC> codec = std::make_shared<KEY_CODEC>())
        : raw_kafka_sink_base<K, void, KEY_CODEC, void>(config, topic, codec, nullptr) {
    }

    ~raw_kafka_sink() override {
      this->close();
    }

    inline void produce(std::shared_ptr<const krecord<K, void>> r, std::function<void(int64_t offset, int32_t ec)> callback) {
      auto am = std::make_shared<commit_chain::autocommit_marker>(callback);
      this->_queue.push_back(std::make_shared<kevent<K, void>>(r, am));
    }

    inline void produce(uint32_t partition_hash, std::shared_ptr<const krecord<K, void>> r, std::function<void(int64_t offset, int32_t ec)> callback) {
      auto am = std::make_shared<commit_chain::autocommit_marker>(callback);
      this->_queue.push_back(std::make_shared<kevent<K, void>>(r, am, partition_hash));
    }

    inline void produce(const K &key, int64_t ts, std::function<void(int64_t offset, int32_t ec)> callback) {
      produce(std::make_shared<const krecord<K, void>>(key, ts), callback);
    }

    inline void
    produce(uint32_t partition_hash, const K &key, int64_t ts, std::function<void(int64_t offset, int32_t ec)> callback) {
      produce(partition_hash, std::make_shared<const krecord<K, void>>(key, ts), callback);
    }

  protected:
    int handle_event(std::shared_ptr<kevent<K, void>> ev) override {
      uint32_t partition_hash = 0;

      // first time??
      // register schemas under the topic-key, topic-value name to comply with kafka-connect behavior
      if (this->_key_schema_id<0) {
        this->_key_schema_id = this->_key_codec->register_schema(this->topic() + "-key", ev->record()->key());
        LOG_IF(FATAL, this->_key_schema_id<0) << "Failed to register schema - aborting";
      }

      if (ev->has_partition_hash())
        partition_hash = ev->partition_hash();
      else
        partition_hash = (this->_partitioner) ? this->_partitioner(ev->record()->key()) : kspp::get_partition_hash(
            ev->record()->key(), this->_key_codec);

      void *kp = nullptr;
      size_t ksize = 0;

      std::stringstream ks;
      ksize = this->_key_codec->encode(ev->record()->key(), ks);
      kp = malloc(ksize);  // must match the free in kafka_producer TBD change to new[] and a memory pool
      ks.read((char *) kp, ksize);

      return this->_impl.produce(partition_hash, kafka_producer::FREE, kp, ksize, nullptr, 0, ev->event_time(),
                                 ev->id());
    }
  };
}

