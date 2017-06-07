#include <memory>
#include <strstream>
#include <boost/filesystem.hpp>
#include <boost/log/trivial.hpp>
#include <kspp/kspp.h>
#include <kspp/impl/sources/kafka_consumer.h>
#pragma once

//#define LOGPREFIX_ERROR BOOST_LOG_TRIVIAL(error) << BOOST_CURRENT_FUNCTION << this->processor_name() << "-" <<  CODEC::name()
#define KSPP_LOG_NAME this->simple_name() << "-" << CODEC::name()

namespace kspp {
  template<class K, class V, class CODEC>
  class kafka_source_base : public partition_source<K, V>
  {
  public:
    virtual ~kafka_source_base() {
      close();
    }

    virtual std::string simple_name() const {
      return "kafka_source(" + _consumer.topic() + ")";
    }

    /*
    virtual std::string full_name() const {
      return "kafka_source(" + _consumer.topic() + "#" + std::to_string(_consumer.partition()) + ")-codec(" + CODEC::name() + ")[" + type_name<K>::get() + ", " + type_name<V>::get() + "]";
    }
    */

    virtual void start(int64_t offset) {
      _consumer.start(offset);
    }

    virtual void start() {
      _consumer.start();
    }

    virtual void close() {
      if (_commit_chain.last_good_offset()>=0)
        _consumer.commit(_commit_chain.last_good_offset(), true);
      _consumer.close();
    }

    virtual inline bool eof() const {
      return _consumer.eof();
    }

    virtual void commit(bool flush) {
      if (_commit_chain.last_good_offset()>0)
        _consumer.commit(_commit_chain.last_good_offset(), flush);
    }

    virtual size_t queue_len() {
      return 0;
    }
     
    virtual bool process_one(int64_t tick) {
      auto p = _consumer.consume();
      if (!p)
        return false;
      ++_in_count;
      _lag.add_event_time(tick, p->timestamp().timestamp);
      this->send_to_sinks(parse(p));
      return true;
    }

    std::string topic() const {
      return _consumer.topic();
    }

  protected:
  kafka_source_base(std::string brokers, std::string topic, int32_t partition, std::string consumer_group, std::chrono::milliseconds max_buffering_time, std::shared_ptr<CODEC> codec)
    : partition_source<K, V>(nullptr, partition)
    , _codec(codec)
    , _consumer(brokers, topic, partition, consumer_group, max_buffering_time)
    , _commit_chain(topic, partition)
    , _in_count("in_count")
    , _commit_chain_size("commit_chain_size", [this]() { return _commit_chain.size(); })
    , _lag() {
      this->add_metric(&_in_count);
      this->add_metric(&_commit_chain_size);
      this->add_metric(&_lag);
    }

    virtual std::shared_ptr<kevent<K, V>> parse(const std::unique_ptr<RdKafka::Message> & ref) = 0;

    kafka_consumer         _consumer;
    std::shared_ptr<CODEC> _codec;
    commit_chain           _commit_chain;
    metric_counter         _in_count;
    metric_evaluator       _commit_chain_size;
    metric_lag             _lag;
  };

  template<class K, class V, class CODEC>
  class kafka_source : public kafka_source_base<K, V, CODEC>
  {
  public:
    kafka_source(topology_base& topology, std::string topic, std::shared_ptr<CODEC> codec = std::make_shared<CODEC>())
      : kafka_source_base<K, V, CODEC>(topology.brokers(), topic, topology.partition(), topology.group_id(), topology.max_buffering_time(), codec) {}

    kafka_source(topology_base& topology, int32_t partition, std::string topic, std::shared_ptr<CODEC> codec = std::make_shared<CODEC>())
      : kafka_source_base<K, V, CODEC>(topology.brokers(), topic, partition, topology.group_id(), topology.max_buffering_time(), codec) {}

  protected:
    std::shared_ptr<kevent<K, V>> parse(const std::unique_ptr<RdKafka::Message> & ref) {
      if (!ref)
        return nullptr;

      int64_t timestamp = (ref->timestamp().timestamp >= 0) ? ref->timestamp().timestamp : milliseconds_since_epoch();
      K tmp_key;
      {

        size_t consumed = this->_codec->decode((const char*) ref->key_pointer(), ref->key_len(), tmp_key);
        if (consumed == 0) {
          BOOST_LOG_TRIVIAL(error) << KSPP_LOG_NAME << ", decode key failed, actual key sz:" << ref->key_len();
          return nullptr;
        } else if (consumed != ref->key_len()) {
          BOOST_LOG_TRIVIAL(error) << KSPP_LOG_NAME << ", decode key failed, consumed: " << consumed << ", actual: " << ref->key_len();
          return nullptr;
        }
      }

      std::shared_ptr<V> tmp_value = nullptr;

      size_t sz = ref->len();
      if (sz) {
        tmp_value = std::make_shared<V>();
        size_t consumed = this->_codec->decode((const char*) ref->payload(), sz, *tmp_value);
        if (consumed == 0) {
          BOOST_LOG_TRIVIAL(error) << KSPP_LOG_NAME << ", decode value failed, size:" << sz;
          return nullptr;
        } else if (consumed != sz) {
          BOOST_LOG_TRIVIAL(error) << KSPP_LOG_NAME << ", decode value failed, consumed: " << consumed << ", actual: " << sz;
          return nullptr;
        }
      }
      auto record = std::make_shared<krecord<K, V>>(tmp_key, tmp_value, timestamp);
      return std::make_shared<kevent<K, V>>(record, this->_commit_chain.create(ref->offset()));
    }
  };

  // <void, VALUE>
  template<class V, class CODEC>
  class kafka_source<void, V, CODEC> : public kafka_source_base<void, V, CODEC>
  {
  public:
    kafka_source(topology_base& topology, std::string topic, std::shared_ptr<CODEC> codec = std::make_shared<CODEC>())
      : kafka_source_base<void, V, CODEC>(topology.brokers(), topic, topology.partition(), topology.group_id(), topology.max_buffering_time(), codec) {
    }

    kafka_source(topology_base& topology, int32_t partition, std::string topic, std::shared_ptr<CODEC> codec = std::make_shared<CODEC>())
      : kafka_source_base<void, V, CODEC>(topology.brokers(), topic, partition, topology.group_id(), topology.max_buffering_time(), codec) {
    }

  protected:
    std::shared_ptr<kevent<void, V>> parse(const std::unique_ptr<RdKafka::Message> & ref) {
      if (!ref)
        return nullptr;
      size_t sz = ref->len();
      if (sz) {
        int64_t timestamp = (ref->timestamp().timestamp >= 0) ? ref->timestamp().timestamp : milliseconds_since_epoch();
        std::shared_ptr<V> tmp_value = std::make_shared<V>();
        size_t consumed = this->_codec->decode((const char*) ref->payload(), sz, *tmp_value);
        if (consumed == sz) {
          auto record = std::make_shared<krecord<void, V>>(tmp_value, timestamp);
          return std::make_shared<kevent<void, V>>(record, this->_commit_chain.create(ref->offset()));
        }

        if (consumed == 0) {
          BOOST_LOG_TRIVIAL(error) << KSPP_LOG_NAME << ", decode value failed, size:" << sz;
          return nullptr;
        }

        BOOST_LOG_TRIVIAL(error) << KSPP_LOG_NAME << ", decode value failed, consumed: " << consumed << ", actual: " << sz;
        return nullptr;
      }
      return nullptr; // just parsed an empty message???
    }
  };

  //<KEY, nullptr>
  template<class K, class CODEC>
  class kafka_source<K, void, CODEC> : public kafka_source_base<K, void, CODEC>
  {
  public:
    kafka_source(topology_base& topology, std::string topic, std::shared_ptr<CODEC> codec = std::make_shared<CODEC>())
      : kafka_source_base<K, void, CODEC>(topology.brokers(), topic, topology.partition(), topology.group_id(), topology.max_buffering_time(), codec) {}

    kafka_source(topology_base& topology, int32_t partition, std::string topic, std::shared_ptr<CODEC> codec = std::make_shared<CODEC>())
      : kafka_source_base<K, void, CODEC>(topology.brokers(), topic, partition, topology.group_id(), topology.max_buffering_time(), codec) {
    }

  protected:
    std::shared_ptr<kevent<K, void>> parse(const std::unique_ptr<RdKafka::Message> & ref) {
      if (!ref || ref->key_len() == 0)
        return nullptr;

      int64_t timestamp = (ref->timestamp().timestamp >= 0) ? ref->timestamp().timestamp : milliseconds_since_epoch();
      K tmp_key;
      size_t consumed = this->_codec->decode((const char*) ref->key_pointer(), ref->key_len(), tmp_key);
      if (consumed == 0) {
        BOOST_LOG_TRIVIAL(error) << KSPP_LOG_NAME << ", decode key failed, actual key sz:" << ref->key_len();
        return nullptr;
      } else if (consumed != ref->key_len()) {
        BOOST_LOG_TRIVIAL(error) << KSPP_LOG_NAME << ", decode key failed, consumed: " << consumed << ", actual: " << ref->key_len();
        return nullptr;
      }
      auto record = std::make_shared<krecord<K, void>>(tmp_key, timestamp);
      return std::make_shared<kevent<K, void>>(record, this->_commit_chain.create(ref->offset()));
    }
  };
};

