#include <assert.h>
#include <memory>
#include <functional>
#include <sstream>
#include <kspp/kspp.h>
#include <kspp/topology.h>
#include <kspp/internal/sinks/kafka_producer.h>

#pragma once

namespace kspp {
  // SINGLE PARTITION PRODUCER
  // this is just to only override the necessary key value specifications
  template<class K, class V, class KEY_CODEC, class VAL_CODEC>
  class kafka_partition_sink_base : public partition_sink<K, V> {
    static constexpr const char *PROCESSOR_NAME = "kafka_partition_sink";
  protected:
    kafka_partition_sink_base(std::shared_ptr<cluster_config> cconfig,
                              std::string topic,
                              int32_t partition,
                              std::shared_ptr<KEY_CODEC> key_codec,
                              std::shared_ptr<VAL_CODEC> val_codec)
        : partition_sink<K, V>(partition), key_codec_(key_codec), val_codec_(val_codec), fixed_partition_(partition), impl_(cconfig, topic) {
      this->add_metrics_label(KSPP_PROCESSOR_TYPE_TAG, "kafka_partition_sink");
      this->add_metrics_label(KSPP_TOPIC_TAG, topic);
      this->add_metrics_label(KSPP_PARTITION_TAG, std::to_string(partition));
    }

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
      return event_consumer<K, V>::queue_size() + impl_.queue_size();
    }

    size_t outbound_queue_len() const override {
      return impl_.queue_size();
    }

    void poll(int timeout) override {
      return impl_.poll(timeout);
    }

    void commit(bool flush) override {
      // noop
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
    virtual int handle_event(std::shared_ptr<kevent<K, V>>) = 0;

    std::shared_ptr<KEY_CODEC> key_codec_;
    std::shared_ptr<VAL_CODEC> val_codec_;
    int32_t key_schema_id_ = -1;
    int32_t val_schema_id_= -1;
    size_t fixed_partition_;
    kafka_producer impl_;
  };

  template<class K, class V, class KEY_CODEC, class VAL_CODEC>
  class kafka_partition_sink : public kafka_partition_sink_base<K, V, KEY_CODEC, VAL_CODEC> {
  public:
    kafka_partition_sink(std::shared_ptr<cluster_config> config,
                         int32_t partition,
                         std::string topic,
                         std::shared_ptr<KEY_CODEC> key_codec = std::make_shared<KEY_CODEC>(),
                         std::shared_ptr<VAL_CODEC> val_codec = std::make_shared<VAL_CODEC>())
        : kafka_partition_sink_base<K, V, KEY_CODEC, VAL_CODEC>(config,
                                                                topic,
                                                                partition,
                                                                key_codec,
                                                                val_codec) {
    }

    ~kafka_partition_sink() override {
      this->close();
    }

  protected:
    int handle_event(std::shared_ptr<kevent<K, V>> ev) override {
      void *kp = nullptr;
      void *vp = nullptr;
      size_t ksize = 0;
      size_t vsize = 0;

      // first time??
      // register schemas under the topic-key, topic-value name to comply with kafka-connect behavior
      if (this->key_schema_id_ < 0) {
        this->key_schema_id_ = this->key_codec_->register_schema(this->topic() + "-key", ev->record()->key());
        LOG_IF(FATAL, this->key_schema_id_ < 0) << "Failed to register schema - aborting";
      }

      if (this->val_schema_id_ < 0 && ev->record()->value()) {
        this->val_schema_id_ = this->val_codec_->register_schema(this->topic() + "-value", *ev->record()->value());
        LOG_IF(FATAL, this->val_schema_id_ < 0) << "Failed to register schema - aborting";
      }

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
      return this->impl_.produce((uint32_t) this->fixed_partition_, kafka_producer::FREE, kp, ksize, vp, vsize,
                                 ev->event_time(), ev->id());
    }
  };

// value only topic
  template<class V, class VAL_CODEC>
  class kafka_partition_sink<void, V, void, VAL_CODEC> : public kafka_partition_sink_base<void, V, void, VAL_CODEC> {
    static constexpr const char *PROCESSOR_NAME = "kafka_partition_sink";
  public:
    kafka_partition_sink(std::shared_ptr<cluster_config> config,
                         int32_t partition,
                         std::string topic,
                         std::shared_ptr<VAL_CODEC> val_codec = std::make_shared<VAL_CODEC>())
        : kafka_partition_sink_base<void, V, void, VAL_CODEC>(config,
                                                              topic,
                                                              partition,
                                                              nullptr,
                                                              val_codec) {
    }

    ~kafka_partition_sink() override {
      this->close();
    }

  protected:
    int handle_event(std::shared_ptr<kevent<void, V>> ev) override {
      void *vp = nullptr;
      size_t vsize = 0;

      // first time??
      // register schemas under the topic-key, topic-value name to comply with kafka-connect behavior
      if (this->val_schema_id_ < 0 && ev->record()->value()) {
        this->val_schema_id_ = this->_val_codec->register_schema(this->topic() + "-value", *ev->record()->value());
        LOG_IF(FATAL, this->val_schema_id_ < 0) << "Failed to register schema - aborting";
      }

      if (ev->record()->value()) {
        std::stringstream vs;
        vsize = this->val_codec_->encode(*ev->record()->value(), vs);
        vp = malloc(vsize);   // must match the free in kafka_producer TBD change to new[] and a memory pool
        vs.read((char *) vp, vsize);
      } else {
        assert(false);
        return 0; // no writing of null key and null values
      }
      return this->impl_.produce((uint32_t) this->_fixed_partition, kafka_producer::FREE, nullptr, 0, vp, vsize,
                                 ev->event_time(), ev->id());
    }
  };

// key only topic
  template<class K, class KEY_CODEC>
  class kafka_partition_sink<K, void, KEY_CODEC, void> : public kafka_partition_sink_base<K, void, KEY_CODEC, void> {
  public:
    kafka_partition_sink(std::shared_ptr<cluster_config> config,
                         int32_t partition,
                         std::string topic,
                         std::shared_ptr<KEY_CODEC> key_codec = std::make_shared<KEY_CODEC>())
        : kafka_partition_sink_base<K, void, KEY_CODEC, void>(config,
                                                              topic,
                                                              partition,
                                                              key_codec,
                                                              nullptr) {
    }

    ~kafka_partition_sink() override {
      this->close();
    }

  protected:
    int handle_event(std::shared_ptr<kevent<K, void>> ev) override {

      // first time??
      // register schemas under the topic-key, topic-value name to comply with kafka-connect behavior
      if (this->key_schema_id_ < 0) {
        this->key_schema_id_ = this->key_codec_->register_schema(this->topic() + "-key", ev->record()->key());
        LOG_IF(FATAL, this->key_schema_id_ < 0) << "Failed to register schema - aborting";
      }

      void *kp = nullptr;
      size_t ksize = 0;
      std::stringstream ks;
      //ksize = this->_codec->encode(ev->record()->key(), ks);
      ksize = this->key_codec_->encode(ev->record()->key(), ks);
      kp = malloc(ksize);  // must match the free in kafka_producer TBD change to new[] and a memory pool
      ks.read((char *) kp, ksize);
      return this->impl_.produce((uint32_t) this->fixed_partition_,
                                 kafka_producer::FREE,
                                 kp,
                                 ksize,
                                 nullptr,
                                 0,
                                 ev->event_time(),
                                 ev->id());
    }
  };
}

