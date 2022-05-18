#include <assert.h>
#include <memory>
#include <functional>
#include <sstream>
#include <kspp/kspp.h>
#include <kspp/topology.h>
#include <kspp/internal/sinks/kafka_producer.h>
#include <kspp/sinks/sink_defs.h>

#pragma once

namespace kspp {
  template<class K, class V, class KEY_CODEC, class VAL_CODEC>
  class kafka_sink_base : public topic_sink<K, V> {
    static constexpr const char *PROCESSOR_NAME = "kafka_sink";
  public:
    enum {
      MAX_KEY_SIZE = 1000
    };

    using partitioner = typename kafka_partitioner_base<K>::partitioner;

    std::string log_name() const override {
      return PROCESSOR_NAME;
    }

    std::string topic() const override {
      return impl_.topic();
    }

    std::string precondition_topic() const override {
      return impl_.topic();
    }

    void close() override {
      flush();
      return impl_.close();
    }

    size_t queue_size() const override {
      return topic_sink<K, V>::queue_size() + impl_.queue_size();
    }

    size_t outbound_queue_len() const override {
      return impl_.queue_size();
    }

    void poll(int timeout) override {
      return impl_.poll(timeout);
    }

    void flush() override {
      while (!eof()) {
        process(kspp::milliseconds_since_epoch());
        poll(0);
      }
      while (true) {
        auto ec = impl_.flush(1000);
        if (ec == 0)
          break;
      }
    }

    bool eof() const override {
      return this->queue_.size() == 0;
    }

    // lets try to get as much as possible from queue to librdkafka - stop when queue is empty or librdkafka fails
    size_t process(int64_t tick) override {
      size_t count = 0;
      while (this->queue_.size()) {
        auto ev = this->queue_.front();
        int ec = handle_event(ev);
        if (ec == 0) {
          ++count;
          ++(this->processed_count_);
          this->lag_.add_event_time(kspp::milliseconds_since_epoch(), ev->event_time()); // move outside loop
          this->queue_.pop_front();
          continue;
        } else if (ec == RdKafka::ERR__QUEUE_FULL) {
          // expected and retriable
          return count;
        } else {
          LOG(ERROR) << "other error from rd_kafka ec:" << ec;
          // permanent failure - need to stop TBD
          return count;
        }
      } // while
      return count;
    }

  protected:
    kafka_sink_base(std::shared_ptr<cluster_config> cconfig,
                    std::string topic,
                    partitioner p,
                    std::shared_ptr<KEY_CODEC> key_codec,
                    std::shared_ptr<VAL_CODEC> val_codec)
        : topic_sink<K, V>(), key_codec_(key_codec), val_codec_(val_codec), impl_(cconfig, topic), partitioner_(p) {
      this->add_metrics_label(KSPP_TOPIC_TAG, topic);
      this->add_metrics_label(KSPP_PROCESSOR_TYPE_TAG, "kafka_sink");
    }

    kafka_sink_base(std::shared_ptr<cluster_config> cconfig,
                    std::string topic,
                    std::shared_ptr<KEY_CODEC> key_codec,
                    std::shared_ptr<VAL_CODEC> val_codec)
        : topic_sink<K, V>(), key_codec_(key_codec), val_codec_(val_codec), impl_(cconfig, topic) {
      this->add_metrics_label(KSPP_TOPIC_TAG, topic);
      this->add_metrics_label(KSPP_PROCESSOR_TYPE_TAG, "kafka_sink");
    }

    virtual int handle_event(std::shared_ptr<kevent<K, V>>) = 0;

    std::shared_ptr<KEY_CODEC> key_codec_;
    std::shared_ptr<VAL_CODEC> val_codec_;
    int32_t key_schema_id_ = -1;
    int32_t val_schema_id_ = -1;
    kafka_producer impl_;
    partitioner partitioner_;
  };

  template<class K, class V, class KEY_CODEC, class VAL_CODEC>
  class kafka_sink : public kafka_sink_base<K, V, KEY_CODEC, VAL_CODEC> {
  public:
    enum {
      MAX_KEY_SIZE = 1000
    };

    using partitioner = typename kafka_partitioner_base<K>::partitioner;

    kafka_sink(std::shared_ptr<cluster_config> config,
               std::string topic,
               partitioner p,
               std::shared_ptr<KEY_CODEC> key_codec = std::make_shared<KEY_CODEC>(),
               std::shared_ptr<VAL_CODEC> val_codec = std::make_shared<VAL_CODEC>())
        : kafka_sink_base<K, V, KEY_CODEC, VAL_CODEC>(config,
                                                      topic,
                                                      p,
                                                      key_codec,
                                                      val_codec) {
    }

    kafka_sink(std::shared_ptr<cluster_config> config,
               std::string topic,
               std::shared_ptr<KEY_CODEC> key_codec = std::make_shared<KEY_CODEC>(),
               std::shared_ptr<VAL_CODEC> val_codec = std::make_shared<VAL_CODEC>())
        : kafka_sink_base<K, V, KEY_CODEC, VAL_CODEC>(config,
                                                      topic,
                                                      key_codec,
                                                      val_codec) {
    }

    ~kafka_sink() override {
      this->close();
    }

  protected:
    int handle_event(std::shared_ptr<kevent<K, V>> ev) override {
      if (ev == nullptr)
        return 0;

      // first time??
      // register schemas under the topic-key, topic-value name to comply with kafka-connect behavior
      if (this->key_schema_id_ < 0) {
        //this->key_schema_id_ = this->key_codec_->register_schema(this->topic() + "-key", ev->record()->key());
        this->key_schema_id_ = this->key_codec_->template register_schema<K>(this->topic() + "-key");
        LOG_IF(FATAL, this->key_schema_id_ < 0) << "Failed to register schema - aborting";
      }

      if (this->val_schema_id_ < 0 && ev->record()->value()) {
        this->val_schema_id_ = this->val_codec_->template register_schema<V>(this->topic() + "-value");
        LOG_IF(FATAL, this->val_schema_id_ < 0) << "Failed to register schema - aborting";
      }

      uint32_t partition_hash = 0;

      if (ev->has_partition_hash())
        partition_hash = ev->partition_hash();
      else
        partition_hash = (this->partitioner_) ? this->partitioner_(ev->record()->key()) : kspp::get_partition_hash(
            ev->record()->key(), this->key_codec_);

      void *kp = nullptr;
      void *vp = nullptr;
      size_t ksize = 0;
      size_t vsize = 0;

      std::stringstream ks;
      ksize = this->key_codec_->encode(ev->record()->key(), ks);
      kp = malloc(ksize);  // must match the free in kafka_producer TBD change to new[] and a memory pool
      ks.read((char *) kp, ksize);

      if (ev->record()->value()) {
        std::stringstream vs;
        vsize = this->val_codec_->encode(*ev->record()->value(), vs);
        vp = malloc(vsize);   // must match the free in kafka_producer TBD change to new[] and a memory pool
        vs.read((char *) vp, vsize);
      }
      return this->impl_.produce(partition_hash, kafka_producer::FREE, kp, ksize, vp, vsize, ev->event_time(),
                                 ev->id());
    }
  };

//<null, VALUE>
  template<class V, class VAL_CODEC>
  class kafka_sink<void, V, void, VAL_CODEC> : public kafka_sink_base<void, V, void, VAL_CODEC> {
  public:
    kafka_sink(std::shared_ptr<cluster_config> config,
               std::string topic,
               std::shared_ptr<VAL_CODEC> val_codec = std::make_shared<VAL_CODEC>())
        : kafka_sink_base<void, V, void, VAL_CODEC>(config, topic, nullptr, val_codec) {
    }

    ~kafka_sink() override {
      this->close();
    }

  protected:
    int handle_event(std::shared_ptr<kevent<void, V>> ev) override {
      if (ev == nullptr)
        return 0;

      // first time??
      // register schemas under the topic-key, topic-value name to comply with kafka-connect behavior
      if (this->val_schema_id_ < 0 && ev->record()->value()) {
        //this->val_schema_id_ = this->val_codec_->register_schema<V>(this->topic() + "-value", *ev->record()->value());
        this->val_schema_id_ = this->val_codec_->template register_schema<V>(this->topic() + "-value");
        LOG_IF(FATAL, this->val_schema_id_ < 0) << "Failed to register schema - aborting";
      }

      static uint32_t s_partition = 0;
      uint32_t partition_hash = ev->has_partition_hash() ? ev->partition_hash() : ++s_partition;
      void *vp = nullptr;
      size_t vsize = 0;

      if (ev->record()->value()) {
        std::stringstream vs;
        vsize = this->val_codec_->encode(*ev->record()->value(), vs);
        vp = malloc(vsize);   // must match the free in kafka_producer TBD change to new[] and a memory pool
        vs.read((char *) vp, vsize);
      } else {
        assert(false);
        return 0; // no writing of null key and null values
      }
      return this->impl_.produce(partition_hash, kafka_producer::FREE, nullptr, 0, vp, vsize, ev->event_time(),
                                 ev->id());
    }
  };

  // <key, nullptr>
  template<class K, class KEY_CODEC>
  class kafka_sink<K, void, KEY_CODEC, void> : public kafka_sink_base<K, void, KEY_CODEC, void> {
  public:
    using partitioner = typename kafka_partitioner_base<K>::partitioner;

    kafka_sink(std::shared_ptr<cluster_config> config,
               std::string topic,
               partitioner p,
               std::shared_ptr<KEY_CODEC> key_codec = std::make_shared<KEY_CODEC>())
        : kafka_sink_base<K, void, KEY_CODEC, void>(config, topic, p, key_codec, nullptr) {
    }

    kafka_sink(std::shared_ptr<cluster_config> config, std::string topic,
               std::shared_ptr<KEY_CODEC> key_codec = std::make_shared<KEY_CODEC>())
        : kafka_sink_base<K, void, KEY_CODEC, void>(config, topic, key_codec, nullptr) {
    }

    ~kafka_sink() override {
      this->close();
    }

  protected:
    int handle_event(std::shared_ptr<kevent<K, void>> ev) override {
      if (ev == nullptr)
        return 0;

      // first time??
      // register schemas under the topic-key, topic-value name to comply with kafka-connect behavior
      if (this->key_schema_id_ < 0) {
        //this->key_schema_id_ = this->key_codec_->register_schema(this->topic() + "-key", ev->record()->key());
        this->key_schema_id_ = this->key_codec_->template register_schema<K>(this->topic() + "-key");
        LOG_IF(FATAL, this->key_schema_id_ < 0) << "Failed to register schema - aborting";
      }

      uint32_t partition_hash = 0;
      if (ev->has_partition_hash())
        partition_hash = ev->partition_hash();
      else
        partition_hash = (this->partitioner_) ? this->partitioner_(ev->record()->key()) : kspp::get_partition_hash(
            ev->record()->key(), this->key_codec_);

      void *kp = nullptr;
      size_t ksize = 0;

      std::stringstream ks;
      ksize = this->key_codec_->encode(ev->record()->key(), ks);
      kp = malloc(ksize);  // must match the free in kafka_producer TBD change to new[] and a memory pool
      ks.read((char *) kp, ksize);

      return this->impl_.produce(partition_hash, kafka_producer::FREE, kp, ksize, nullptr, 0, ev->event_time(),
                                 ev->id());
    }
  };
}

