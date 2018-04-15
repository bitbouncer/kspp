#include <kspp/kspp.h>
#pragma once
namespace kspp {

  template<class K, class V>
  class merge : public event_consumer<K, V>, public partition_source<K, V> {
    static constexpr const char* PROCESSOR_NAME = "merge";
  public:
    typedef K key_type;
    typedef V value_type;
    typedef kspp::kevent<K, V> record_type;

    // fix this so source must be descendant from partition source...
    template<class source>
    merge(topology &unused, const std::vector<std::shared_ptr<source>>& upstream, int32_t partition=-1)
        : event_consumer<K, V>()
        , partition_source<K, V>(nullptr, partition) {
      this->add_metrics_tag(KSPP_PROCESSOR_TYPE_TAG, "merge");
      for (auto&& i : upstream) {
        i->add_sink([this](auto e) {
          this->_queue.push_back(e);
        });
      }
    }

    std::string log_name() const override {
      return PROCESSOR_NAME;
    }

    size_t process(int64_t tick) override {
      size_t processed = 0;
      for (auto i : this->upstream_)
        i->process(tick);

      while (this->_queue.next_event_time()<=tick) {
        auto trans = this->_queue.pop_and_get();
        this->send_to_sinks(trans);
        ++processed;
      }
      return processed;
    }

    size_t queue_size() const override {
      return event_consumer<K, V>::queue_size();
    }

    int64_t next_event_time() const override {
      return event_consumer<K, V>::next_event_time();
    }

    void commit(bool force) override {
      for (auto i : this->upstream_)
        i->commit(force);
    }

    // do we have the right hierarchy since those are not overridden and they should????
    int produce(std::shared_ptr<kevent < K, V>> r) {
      this->send_to_sinks(r);
      return 0;
    }

    //int produce(const K &key, const V &value, int64_t ts = kspp::milliseconds_since_epoch()) {
    //  return produce(std::make_shared<kevent<K, V>>(std::make_shared<krecord<K, V>>(key, value, ts)));
    //}
  };

//<null, VALUE>
  template<class V>
  class merge<void, V> : public event_consumer<void, V>, public partition_source<void, V> {
    static constexpr const char* PROCESSOR_NAME = "merge";
  public:
    typedef void key_type;
    typedef V value_type;
    typedef kspp::kevent<void, V> record_type;

    merge(topology &unused, std::vector<partition_source<void, V>*> upstream, int32_t partition=-1)
    : event_consumer<void, V>()
    , partition_source<void, V>(nullptr, partition) {
      this->add_metrics_tag(KSPP_PROCESSOR_TYPE_TAG, "merge");
      for (auto&& i : upstream) {
        i->add_sink([this](auto e) {
          this->_queue.push_back(e);
        });
      }
    }

    std::string simple_name() const override {
      return "merge";
    }

    size_t process(int64_t tick) override {
      size_t processed = 0;
      for (auto i : this->upstream_)
        i->process(tick);

      while (this->_queue.next_event_time()<=tick) {
        auto trans = this->_queue.pop_and_get();
        this->send_to_sinks(trans);
        this->_lag.add_event_time(tick, trans->event_time());
        ++(this->_processed_count);
        ++processed;
      }
      return processed;
    }

    size_t queue_size() const override {
      return event_consumer<void, V>::queue_size();
    }

    int64_t next_event_time() const override {
      return event_consumer<void, V>::next_event_time();
    }

    void commit(bool force) override {
      for (auto i : this->upstream_)
        i->commit(force);
    }

    int produce(std::shared_ptr<kevent < void, V>> r) {
      this->send_to_sinks(r);
      return 0;
    }

    int produce(const V &value) {
      return produce(std::make_shared<kevent<void, V>>(std::make_shared<krecord<void, V>>(value)));
    }

  };

  template<class K>
  class merge<K, void> : public event_consumer<K, void>, public partition_source<K, void> {
    static constexpr const char* PROCESSOR_NAME = "merge";
  public:
    typedef K key_type;
    typedef void value_type;
    typedef kspp::kevent<K, void> record_type;

    merge(topology &unused, std::vector<partition_source<K, void>*> upstream, int32_t partition=-1)
    : event_consumer<K, void>()
    , partition_source<K, void>(nullptr, partition) {
      this->add_metrics_tag(KSPP_PROCESSOR_TYPE_TAG, "merge");
      for (auto&& i : upstream) {
        i->add_sink([this](auto e) { this->_queue.push_back(e); });
      }
    }

    std::string log_name() const override {
      return PROCESSOR_NAME;
    }

    size_t process(int64_t tick) override {
      size_t processed = 0;
      for (auto i : this->upstream_)
        i->process(tick);

      while (this->_queue.next_event_time()<=tick) {
        auto trans = this->_queue.pop_and_get();
        this->send_to_sinks(trans);
        this->_lag.add_event_time(tick, trans->event_time());
        ++(this->_processed_count);
        ++processed;
      }
      return processed;
    }

    size_t queue_size() const override {
      return event_consumer<K, void>::queue_size();
    }

    int64_t next_event_time() const override {
      return event_consumer<K, void>::next_event_time();
    }

    void commit(bool force) override {
      for (auto i : this->upstream_)
        i->commit(force);
    }

    int produce(std::shared_ptr<kevent < K, void>> r) {
      this->send_to_sinks(r);
      return 0;
    }

    int produce(const K &key) {
      return produce(std::make_shared<kevent<K, void>>(std::make_shared<krecord<K, void>>(key)));
    }
  };
}

