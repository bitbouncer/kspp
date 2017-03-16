#include <memory>
#include <strstream>
#include <boost/filesystem.hpp>
#include <boost/log/trivial.hpp>
#include <kspp/kspp.h>
#include <kspp/impl/sources/kafka_consumer.h>
#pragma once

#define LOGPREFIX_ERROR BOOST_LOG_TRIVIAL(error) << BOOST_CURRENT_FUNCTION << this->name()

namespace kspp {
  template<class K, class V, class CODEC>
  class kafka_source_base : public partition_source<K, V>
  {
  public:
    virtual ~kafka_source_base() {
      close();
    }

    virtual std::string name() const {
      return "kafka_source(" + _consumer.topic() + "#" + std::to_string(_consumer.partition()) + ")-codec(" + CODEC::name() + ")[" + type_name<K>::get() + ", " + type_name<V>::get() + "]";
    }

    virtual std::string processor_name() const {
      return "kafka_source";
    }

    virtual void start(int64_t offset) {
      _consumer.start(offset);
    }

    //TBD get offsets from kafka
    virtual void start() {
      _consumer.start(-2);
    }

    virtual void close() {
      _consumer.close();
    }

    virtual inline bool eof() const {
      return _consumer.eof();
    }

    virtual void commit(bool flush) {
      // TBD!!!!
      //_can_be_committed
    }

    virtual size_t queue_len() {
      return 0;
    }
     
    virtual bool process_one(int64_t tick) {
      auto p = _consumer.consume();
      if (!p)
        return false;
      this->send_to_sinks(parse(p));
      return true;
    }

    std::string topic() const {
      return _consumer.topic();
    }

  protected:
    kafka_source_base(std::string brokers, std::string topic, size_t partition, std::shared_ptr<CODEC> codec)
      : partition_source<K, V>(nullptr, partition)
      , _codec(codec)
      , _consumer(brokers, topic, partition)
      , _can_be_committed(-1)
      , _commit_chain([this](int64_t offset) { 
      _can_be_committed = offset; 
      //BOOST_LOG_TRIVIAL(info) << "_can_be_committed " << _can_be_committed;
    }) {
    }

    virtual std::shared_ptr<ktransaction<K, V>> parse(const std::unique_ptr<RdKafka::Message> & ref) = 0;

    kafka_consumer          _consumer;
    std::shared_ptr<CODEC>  _codec;
    int64_t                 _can_be_committed;
    commit_chain            _commit_chain;
  };

  template<class K, class V, class CODEC>
  class kafka_source : public kafka_source_base<K, V, CODEC>
  {
  public:
    kafka_source(topology_base& topology, std::string topic, std::shared_ptr<CODEC> codec = std::make_shared<CODEC>())
      : kafka_source_base<K, V, CODEC>(topology.brokers(), topic, topology.partition(), codec) {}

    kafka_source(topology_base& topology, size_t partition, std::string topic, std::shared_ptr<CODEC> codec = std::make_shared<CODEC>())
      : kafka_source_base<K, V, CODEC>(topology.brokers(), topic, partition, codec) {}

  protected:
    std::shared_ptr<ktransaction<K, V>> parse(const std::unique_ptr<RdKafka::Message> & ref) {
      if (!ref)
        return nullptr;

      auto record = std::make_shared<krecord<K, V>>();
      record->event_time = ref->timestamp().timestamp;
      {
        std::istrstream ks((const char*)ref->key_pointer(), ref->key_len());
        size_t consumed = this->_codec->decode(ks, record->key);
        if (consumed == 0) {
          LOGPREFIX_ERROR << ", decode key failed, actual key sz:" << ref->key_len();
          return nullptr;
        } else if (consumed != ref->key_len()) {
          LOGPREFIX_ERROR << ", decode key failed, consumed: " << consumed << ", actual: " << ref->key_len();
          return nullptr;
        }
      }

      size_t sz = ref->len();
      if (sz) {
        std::istrstream vs((const char*)ref->payload(), sz);
        record->value = std::make_shared<V>();
        size_t consumed = this->_codec->decode(vs, *record->value);
        if (consumed == 0) {
          LOGPREFIX_ERROR << ", decode value failed, size:" << sz;
          return nullptr;
        } else if (consumed != sz) {
          LOGPREFIX_ERROR << ", decode value failed, consumed: " << consumed << ", actual: " << sz;
          return nullptr;
        }
      }
      auto transaction = std::make_shared<ktransaction<K, V>>(record);
      transaction->_commit_callback = this->_commit_chain.create(ref->offset());
      return transaction;
    }
  };

  // <void, VALUE>
  template<class V, class CODEC>
  class kafka_source<void, V, CODEC> : public kafka_source_base<void, V, CODEC>
  {
  public:
    kafka_source(topology_base& topology, std::string topic, std::shared_ptr<CODEC> codec = std::make_shared<CODEC>())
      : kafka_source_base<void, V, CODEC>(topology.brokers(), topic, topology.partition(), codec) {
    }

    kafka_source(topology_base& topology, size_t partition, std::string topic, std::shared_ptr<CODEC> codec = std::make_shared<CODEC>())
      : kafka_source_base<void, V, CODEC>(topology.brokers(), topic, partition, codec) {
    }

  protected:
    std::shared_ptr<ktransaction<void, V>> parse(const std::unique_ptr<RdKafka::Message> & ref) {
      if (!ref)
        return nullptr;
      size_t sz = ref->len();
      if (sz) {
        auto record = std::make_shared<krecord<void, V>>();
        record->event_time = ref->timestamp().timestamp;

        std::istrstream vs((const char*)ref->payload(), sz);
        record->value = std::make_shared<V>();
        size_t consumed = this->_codec->decode(vs, *record->value);
        if (consumed == sz) {
          auto transaction = std::make_shared<ktransaction<void, V>>(record);
          transaction->_commit_callback = this->_commit_chain.create(ref->offset());
          return transaction;
        }

        if (consumed == 0) {
          LOGPREFIX_ERROR << ", decode value failed, size:" << sz;
          return nullptr;
        }

        LOGPREFIX_ERROR << ", decode value failed, consumed: " << consumed << ", actual: " << sz;
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
      : kafka_source_base<K, void, CODEC>(topology.brokers(), topic, topology.partition(), codec) {}

    kafka_source(topology_base& topology, size_t partition, std::string topic, std::shared_ptr<CODEC> codec = std::make_shared<CODEC>())
      : kafka_source_base<K, void, CODEC>(topology.brokers(), topic, partition, codec) {
    }

  protected:
    std::shared_ptr<ktransaction<K, void>> parse(const std::unique_ptr<RdKafka::Message> & ref) {
      if (!ref || ref->key_len() == 0)
        return nullptr;

      auto record = std::make_shared<krecord<K, void>>();
      std::istrstream ks((const char*)ref->key_pointer(), ref->key_len());
      size_t consumed = this->_codec->decode(ks, record->key);
      if (consumed == 0) {
        LOGPREFIX_ERROR << ", decode key failed, actual key sz:" << ref->key_len();
        return nullptr;
      } else if (consumed != ref->key_len()) {
        LOGPREFIX_ERROR << ", decode key failed, consumed: " << consumed << ", actual: " << ref->key_len();
        return nullptr;
      }
      auto transaction = std::make_shared<ktransaction<K, void>>(record);
      transaction->_commit_callback = this->_commit_chain.create(ref->offset());
      return transaction;
    }
  };
};

