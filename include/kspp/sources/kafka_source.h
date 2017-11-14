#include <memory>
#include <strstream>
#include <boost/filesystem.hpp>
#include <glog/logging.h>
#include <kspp/kspp.h>
#include <kspp/topology.h>
#include <kspp/impl/sources/kafka_consumer.h>

#pragma once
#define KSPP_LOG_NAME this->simple_name() << "-" << CODEC::name()

namespace kspp {
  template<class K, class V, class CODEC>
  class kafka_source_base : public partition_source<K, V> {
  public:
    virtual ~kafka_source_base() {
      close();
    }

    std::string simple_name() const override {
      return "kafka_source(" + _impl.topic() + ")";
    }

    void start(int64_t offset) override {
      _impl.start(offset);
    }

    void close() override {
      if (_commit_chain.last_good_offset() >= 0 && _impl.commited() < _commit_chain.last_good_offset())
        _impl.commit(_commit_chain.last_good_offset(), true);
      _impl.close();
    }

    bool eof() const override {
      return _impl.eof();
    }

    void commit(bool flush) override {
      if (flush)
      if (_commit_chain.last_good_offset() >= 0)
        _impl.commit(_commit_chain.last_good_offset(), flush);
    }

    // TBD if we store last offset and end of stream offset we can use this...
    size_t queue_len() const override {
      return 0;
    }

    bool process_one(int64_t tick) override {
      auto p = _impl.consume();
      if (!p)
        return false;

      if (_wallclock_filter_ms>0)
      {
        auto max_ts = tick - _wallclock_filter_ms;
        while(p->timestamp().timestamp < max_ts) {
          ++_in_wallclock_skipped;
          if ((p = _impl.consume())==nullptr)
            return false;
        }
      }
      ++_in_count;
      _lag.add_event_time(tick, p->timestamp().timestamp);
      this->send_to_sinks(parse(p));
      return true;
    }

    std::string topic() const override {
      return _impl.topic();
    }

  protected:
    kafka_source_base(std::shared_ptr<cluster_config> config,
                      std::string topic,
                      int32_t partition,
                      std::string consumer_group,
                      int64_t wallclock_filter_ms,
                      std::shared_ptr<CODEC> codec)
        : partition_source<K, V>(nullptr, partition)
        , _impl(config, topic, partition, consumer_group)
        , _codec(codec)
        , _commit_chain(topic, partition)
        , _wallclock_filter_ms(wallclock_filter_ms)
        , _in_wallclock_skipped("in_skipped_count")
        , _in_count("in_count")
        , _commit_chain_size("commit_chain_size", [this]() { return _commit_chain.size(); })
        , _lag() {
      this->add_metric(&_in_wallclock_skipped);
      this->add_metric(&_in_count);
      this->add_metric(&_commit_chain_size);
      this->add_metric(&_lag);
    }

    virtual std::shared_ptr<kevent<K, V>> parse(const std::unique_ptr<RdKafka::Message> &ref) = 0;

    kafka_consumer _impl;
    std::shared_ptr<CODEC> _codec;
    commit_chain _commit_chain;
    int64_t _wallclock_filter_ms;
    metric_counter _in_wallclock_skipped;
    metric_counter _in_count;
    metric_evaluator _commit_chain_size;
    metric_lag _lag;
  };

  template<class K, class V, class CODEC>
  class kafka_source : public kafka_source_base<K, V, CODEC> {
  public:
    kafka_source(topology &t,
                 int32_t partition,
                 std::string topic,
                 std::shared_ptr<CODEC> codec = std::make_shared<CODEC>())
        : kafka_source_base<K, V, CODEC>(
        t.get_cluster_config(),
        topic, partition,
        t.consumer_group(),
        0,
        codec) {
    }

    kafka_source(topology &t,
                 int32_t partition,
                 std::string topic,
                 std::chrono::seconds max_age,
                 std::shared_ptr<CODEC> codec = std::make_shared<CODEC>())
        : kafka_source_base<K, V, CODEC>(
        t.get_cluster_config(),
        topic, partition,
        t.consumer_group(),
        max_age.count() * 1000,
        codec) {
    }

  protected:
    std::shared_ptr<kevent<K, V>> parse(const std::unique_ptr<RdKafka::Message> &ref) override {
      if (!ref)
        return nullptr;

      int64_t timestamp = (ref->timestamp().timestamp >= 0) ? ref->timestamp().timestamp : milliseconds_since_epoch();

      if (ref->key_len()==0) {
        LOG_EVERY_N(WARNING, 100)
          << KSPP_LOG_NAME
          << ", skipping item with empty key ("
          << google::COUNTER
          << ")";
        return nullptr;
      }

      K tmp_key;
      {

        size_t consumed = this->_codec->decode((const char *) ref->key_pointer(), ref->key_len(), tmp_key);
        if (consumed == 0) {
          LOG_IF(ERROR, ref->key_len()!=0) << KSPP_LOG_NAME << ", decode key failed, actual key sz:" << ref->key_len();
          return nullptr;
        } else if (consumed != ref->key_len()) {
          LOG(ERROR)
              << KSPP_LOG_NAME
              << ", decode key failed, consumed: "
              << consumed
              << ", actual: "
              << ref->key_len();
          return nullptr;
        }
      }

      std::shared_ptr<V> tmp_value = nullptr;

      size_t sz = ref->len();
      if (sz) {
        tmp_value = std::make_shared<V>();
        size_t consumed = this->_codec->decode((const char *) ref->payload(), sz, *tmp_value);
        if (consumed == 0) {
          LOG(ERROR) << KSPP_LOG_NAME << ", decode value failed, size:" << sz;
          return nullptr;
        } else if (consumed != sz) {
          LOG(ERROR) << KSPP_LOG_NAME << ", decode value failed, consumed: " << consumed << ", actual: "
                     << sz;
          return nullptr;
        }
      }
      auto record = std::make_shared<krecord<K, V>>(tmp_key, tmp_value, timestamp);
      return std::make_shared<kevent<K, V>>(record, this->_commit_chain.create(ref->offset()));
    }
  };

  // <void, VALUE>
  template<class V, class CODEC>
  class kafka_source<void, V, CODEC> : public kafka_source_base<void, V, CODEC> {
  public:
    kafka_source(topology &t,
                 int32_t partition,
                 std::string topic,
                 std::shared_ptr<CODEC> codec = std::make_shared<CODEC>())
        : kafka_source_base<void, V, CODEC>(t.get_cluster_config(), topic, partition, t.consumer_group(), 0, codec) {
    }

    kafka_source(topology &t,
                 int32_t partition,
                 std::string topic,
                 std::chrono::seconds max_age,
                 std::shared_ptr<CODEC> codec = std::make_shared<CODEC>())
        : kafka_source_base<void, V, CODEC>(
        t.get_cluster_config(),
        topic, partition,
        t.consumer_group(),
        max_age.count() * 1000,
        codec) {
    }

  protected:
    std::shared_ptr<kevent<void, V>> parse(const std::unique_ptr<RdKafka::Message> &ref) override {
      if (!ref)
        return nullptr;
      size_t sz = ref->len();
      if (sz) {
        int64_t timestamp = (ref->timestamp().timestamp >= 0) ? ref->timestamp().timestamp : milliseconds_since_epoch();
        std::shared_ptr<V> tmp_value = std::make_shared<V>();
        size_t consumed = this->_codec->decode((const char *) ref->payload(), sz, *tmp_value);
        if (consumed == sz) {
          auto record = std::make_shared<krecord<void, V>>(tmp_value, timestamp);
          return std::make_shared<kevent<void, V>>(record, this->_commit_chain.create(ref->offset()));
        }

        if (consumed == 0) {
          LOG(ERROR) << KSPP_LOG_NAME << ", decode value failed, size:" << sz;
          return nullptr;
        }

        LOG(ERROR) << KSPP_LOG_NAME << ", decode value failed, consumed: " << consumed << ", actual: " << sz;
        return nullptr;
      }
      return nullptr; // just parsed an empty message???
    }
  };

  //<KEY, nullptr>
  template<class K, class CODEC>
  class kafka_source<K, void, CODEC> : public kafka_source_base<K, void, CODEC> {
  public:
    kafka_source(topology &t,
                 int32_t partition,
                 std::string topic,
                 std::shared_ptr<CODEC> codec = std::make_shared<CODEC>())
        : kafka_source_base<K, void, CODEC>(t.get_cluster_config(), topic, partition, t.consumer_group(), 0, codec) {
    }

    kafka_source(topology &t,
                 int32_t partition,
                 std::string topic,
                 std::chrono::seconds max_age,
                 std::shared_ptr<CODEC> codec = std::make_shared<CODEC>())
        : kafka_source_base<K, void, CODEC>(
        t.get_cluster_config(),
        topic, partition,
        t.consumer_group(),
        max_age * 1000,
        codec) {
    }

  protected:
    std::shared_ptr<kevent<K, void>> parse(const std::unique_ptr<RdKafka::Message> &ref) override {
      if (!ref || ref->key_len() == 0)
        return nullptr;

      int64_t timestamp = (ref->timestamp().timestamp >= 0) ? ref->timestamp().timestamp : milliseconds_since_epoch();
      K tmp_key;
      size_t consumed = this->_codec->decode((const char *) ref->key_pointer(), ref->key_len(), tmp_key);
      if (consumed == 0) {
        LOG(ERROR) << KSPP_LOG_NAME << ", decode key failed, actual key sz:" << ref->key_len();
        return nullptr;
      } else if (consumed != ref->key_len()) {
        LOG(ERROR) << KSPP_LOG_NAME << ", decode key failed, consumed: " << consumed << ", actual: "
                   << ref->key_len();
        return nullptr;
      }
      auto record = std::make_shared<krecord<K, void>>(tmp_key, timestamp);
      return std::make_shared<kevent<K, void>>(record, this->_commit_chain.create(ref->offset()));
    }
  };
}

