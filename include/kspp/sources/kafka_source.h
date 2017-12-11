#include <kspp/kspp.h>
#include <memory>
#include <strstream>
#include <thread>
#include <glog/logging.h>
#include <kspp/topology.h>
#include <kspp/impl/sources/kafka_consumer.h>

#pragma once
//#define KSPP_LOG_NAME this->simple_name() << "-" << CODEC::name()
#define KSPP_LOG_NAME this->simple_name()

namespace kspp {
  template<class K, class V, class KEY_CODEC, class VAL_CODEC>
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
      _started = true;
    }

    void close() override {
      if (!_exit) {
        _exit = true;
        _thread.join();
      }

      if (_commit_chain.last_good_offset() >= 0 && _impl.commited() < _commit_chain.last_good_offset())
        _impl.commit(_commit_chain.last_good_offset(), true);
      _impl.close();
    }

    bool eof() const override {
      return _incomming_msg.size()==0 && _impl.eof();
    }

    void commit(bool flush) override {
      if (_commit_chain.last_good_offset() >= 0)
        _impl.commit(_commit_chain.last_good_offset(), flush);
    }

    // TBD if we store last offset and end of stream offset we can use this...
    size_t queue_size() const override {
      return _incomming_msg.size();
    }

    bool process_one(int64_t tick) override {
      if (_incomming_msg.size() == 0)
        return false;
      auto p = _incomming_msg.front();
      _incomming_msg.pop_front();
      ++_in_count;
      _lag.add_event_time(tick, p->event_time());
      this->send_to_sinks(p);
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
                      std::chrono::system_clock::time_point start_point,
                      std::shared_ptr<KEY_CODEC> key_codec,
                      std::shared_ptr<VAL_CODEC> val_codec)
        : partition_source<K, V>(nullptr, partition)
        , _started(false)
        , _exit(false)
        , _thread(&kafka_source_base::thread_f, this)
        , _impl(config, topic, partition, consumer_group)
        , _key_codec(key_codec)
        , _val_codec(val_codec)
        , _commit_chain(topic, partition)
        , _start_point_ms(std::chrono::time_point_cast<std::chrono::milliseconds>(start_point).time_since_epoch().count())
        , _parse_errors("parse_errors")
        , _in_count("in_count")
        , _commit_chain_size("commit_chain_size", [this]() { return _commit_chain.size(); })
        , _lag() {
      this->add_metric(&_in_count);
      this->add_metric(&_commit_chain_size);
      this->add_metric(&_lag);
    }

    virtual std::shared_ptr<kevent<K, V>> parse(const std::unique_ptr<RdKafka::Message> &ref) = 0;


    void thread_f()
    {
      while(!_started)
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
      DLOG(INFO) << "starting thread";

      if (_start_point_ms>0) {
        DLOG(INFO) << "spooling phase";
        bool done_skipping =false;
        while (!_exit && !done_skipping) {
          while (auto p = _impl.consume()) {
            if (p->timestamp().timestamp < _start_point_ms) {
            } else {
              done_skipping = true;
              // we need to sent the first message to the queue
              auto decoded_msg = parse(p);
              if (decoded_msg) {
                _incomming_msg.push_back(decoded_msg);
              } else {
                ++_parse_errors;
              }
            }
          }
          std::this_thread::sleep_for(std::chrono::milliseconds(10)); // wait for more messages
        }
      }

      DLOG(INFO) << "consumption phase";

      while(!_exit) {
        auto tick = kspp::milliseconds_since_epoch();
        while (auto p = _impl.consume()) {
          auto decoded_msg = parse(p);
          if (decoded_msg) {
            _incomming_msg.push_back(decoded_msg);
          } else {
            ++_parse_errors;
          }

          // to much work in queue - back off and let the conumers work
          while(_incomming_msg.size()>1000 && !_exit)
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
      }
      DLOG(INFO) << "exiting thread";
    }

    bool _started;
    bool _exit;
    std::thread _thread;
    event_queue<K, V> _incomming_msg;
    kafka_consumer _impl;
    std::shared_ptr<KEY_CODEC> _key_codec;
    std::shared_ptr<VAL_CODEC> _val_codec;
    commit_chain _commit_chain;
    int64_t _start_point_ms;
    metric_counter _parse_errors;
    metric_counter _in_count;
    metric_evaluator _commit_chain_size;
    metric_lag _lag;
  };

  template<class K, class V,  class KEY_CODEC, class VAL_CODEC>
  class kafka_source : public kafka_source_base<K, V, KEY_CODEC, VAL_CODEC> {
  public:
    kafka_source(topology &t,
                 int32_t partition,
                 std::string topic,
                 std::shared_ptr<KEY_CODEC> key_codec = std::make_shared<KEY_CODEC>(),
                 std::shared_ptr<VAL_CODEC> val_codec = std::make_shared<VAL_CODEC>())
        : kafka_source_base<K, V, KEY_CODEC, VAL_CODEC>(
        t.get_cluster_config(),
        topic, partition,
        t.consumer_group(),
        std::chrono::system_clock::from_time_t(0),
        key_codec,
        val_codec) {
    }

    kafka_source(topology &t,
                 int32_t partition,
                 std::string topic,
                 std::chrono::system_clock::time_point start_point,
                 std::shared_ptr<KEY_CODEC> key_codec = std::make_shared<KEY_CODEC>(),
                 std::shared_ptr<VAL_CODEC> val_codec = std::make_shared<VAL_CODEC>())
        : kafka_source_base<K, V, KEY_CODEC, VAL_CODEC>(
        t.get_cluster_config(),
        topic, partition,
        t.consumer_group(),
        start_point,
        key_codec,
        val_codec) {
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

        size_t consumed = this->_key_codec->decode((const char *) ref->key_pointer(), ref->key_len(), tmp_key);
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
        size_t consumed = this->_val_codec->decode((const char *) ref->payload(), sz, *tmp_value);
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
  template<class V, class VAL_CODEC>
  class kafka_source<void, V, void, VAL_CODEC> : public kafka_source_base<void, V, void, VAL_CODEC> {
  public:
    kafka_source(topology &t,
                 int32_t partition,
                 std::string topic,
                 std::shared_ptr<VAL_CODEC> val_codec = std::make_shared<VAL_CODEC>())
        : kafka_source_base<void, V, void, VAL_CODEC>(
        t.get_cluster_config(),
        topic,
        partition,
        t.consumer_group(),
        std::chrono::system_clock::from_time_t(0),
        nullptr,
        val_codec) {
    }

    kafka_source(topology &t,
                 int32_t partition,
                 std::string topic,
                 std::chrono::system_clock::time_point start_point,
                 std::shared_ptr<VAL_CODEC> val_codec = std::make_shared<VAL_CODEC>())
        : kafka_source_base<void, V, void, VAL_CODEC>(
        t.get_cluster_config(),
        topic, partition,
        t.consumer_group(),
        start_point,
        nullptr,
        val_codec) {
    }

  protected:
    std::shared_ptr<kevent<void, V>> parse(const std::unique_ptr<RdKafka::Message> &ref) override {
      if (!ref)
        return nullptr;
      size_t sz = ref->len();
      if (sz) {
        int64_t timestamp = (ref->timestamp().timestamp >= 0) ? ref->timestamp().timestamp : milliseconds_since_epoch();
        std::shared_ptr<V> tmp_value = std::make_shared<V>();
        size_t consumed = this->_val_codec->decode((const char *) ref->payload(), sz, *tmp_value);
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
  template<class K, class KEY_CODEC>
  class kafka_source<K, void, KEY_CODEC, void> : public kafka_source_base<K, void, KEY_CODEC, void> {
  public:
    kafka_source(topology &t,
                 int32_t partition,
                 std::string topic,
                 std::shared_ptr<KEY_CODEC> key_codec = std::make_shared<KEY_CODEC>())
        : kafka_source_base<K, void, KEY_CODEC, void>(
        t.get_cluster_config(),
        topic, partition,
        t.consumer_group(),
        std::chrono::system_clock::from_time_t(0),
        key_codec,
        nullptr) {
    }

    kafka_source(topology &t,
                 int32_t partition,
                 std::string topic,
                 std::chrono::system_clock::time_point start_point,
                 std::shared_ptr<KEY_CODEC> key_codec = std::make_shared<KEY_CODEC>())
        : kafka_source_base<K, void, KEY_CODEC, void>(
        t.get_cluster_config(),
        topic, partition,
        t.consumer_group(),
        start_point,
        key_codec,
        nullptr) {
    }

  protected:
    std::shared_ptr<kevent<K, void>> parse(const std::unique_ptr<RdKafka::Message> &ref) override {
      if (!ref || ref->key_len() == 0)
        return nullptr;

      int64_t timestamp = (ref->timestamp().timestamp >= 0) ? ref->timestamp().timestamp : milliseconds_since_epoch();
      K tmp_key;
      size_t consumed = this->_key_codec->decode((const char *) ref->key_pointer(), ref->key_len(), tmp_key);
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

