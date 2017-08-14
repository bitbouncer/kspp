#include <kspp/kspp.h>
#pragma once

namespace kspp {
  template<class K, class V>
  class pipe : public event_consumer<K, V>, public partition_source<K, V> {
  public:
    typedef K key_type;
    typedef V value_type;
    typedef kspp::kevent<K, V> record_type;

    pipe(topology_base &topology, int32_t partition)
            : event_consumer<K, V>(), partition_source<K, V>(nullptr, partition) {
    }

    pipe(topology_base &topology, std::shared_ptr<kspp::partition_source<K, V>> upstream)
            : event_consumer<K, V>(), partition_source<K, V>(upstream.get(), upstream->partition()) {
      upstream->add_sink([this](auto r) {
        this->send_to_sinks(r);
      });
    }

    pipe(topology_base &topology, std::vector<std::shared_ptr<kspp::partition_source<K, V>>> upstream, int32_t partition)
            : event_consumer<K, V>()
              , partition_source<K, V>(nullptr, partition) {
      for (auto i : upstream) {
        this->add_upstream(i.get());
        i->add_sink([this](auto r) {
          this->send_to_sinks(r);
        });
      }
    }


    std::string simple_name() const override {
      return "pipe";
    }

    bool process_one(int64_t tick) override {
      bool processed = false;
      for (auto i : this->upstream_)
        processed = i->process_one(tick);
      return processed;
    }

    size_t queue_len() const override {
      return event_consumer<K, V>::queue_len();
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

    int produce(const K &key, const V &value, int64_t ts = kspp::milliseconds_since_epoch()) {
      return produce(std::make_shared<kevent<K, V>>(std::make_shared<krecord<K, V>>(key, value, ts)));
    }
  };

//<null, VALUE>
  template<class V>
  class pipe<void, V> : public event_consumer<void, V>, public partition_source<void, V> {
  public:
    typedef void key_type;
    typedef V value_type;
    typedef kspp::kevent<void, V> record_type;

    pipe(topology_base &topology, int32_t partition)
            : event_consumer<void, V>(), partition_source<void, V>(nullptr, partition) {
    }

    pipe(topology_base &topology, std::shared_ptr<kspp::partition_source<void, V>> upstream)
            : event_consumer<void, V>(), partition_source<void, V>(upstream.get(), upstream->partition()) {
      if (upstream)
        upstream->add_sink([this](auto r) {
          this->send_to_sinks(r);
        });
    }

    pipe(topology_base &topology, std::vector<std::shared_ptr<kspp::partition_source<void, V>>> upstream, int32_t partition)
            : event_consumer<void, V>()
              , partition_source<void, V>(nullptr, partition) {
      for (auto i : upstream) {
        this->add_upstream(i.get());
        i->add_sink([this](auto r) {
          this->send_to_sinks(r);
        });
      }
    }

    std::string simple_name() const override {
      return "pipe";
    }

    bool process_one(int64_t tick) override {
      bool processed = false;
      for (auto i : this->upstream_)
        processed = i->process_one(tick);
      return processed;
    }

    size_t queue_len() const override {
      return event_consumer<void, V>::queue_len();
    }

    void commit(bool force) override {
      for (auto i : this->upstream_)
        i->commit(force);
    }


    int produce(std::shared_ptr<kevent < void, V>> r)  {
      this->send_to_sinks(r);
      return 0;
    }

    int produce(const V &value)  {
      return produce(std::make_shared<kevent<void, V>>(std::make_shared<krecord<void, V>>(value)));
    }

  };

  template<class K>
  class pipe<K, void> : public event_consumer<K, void>, public partition_source<K, void> {
  public:
    typedef K key_type;
    typedef void value_type;
    typedef kspp::kevent<K, void> record_type;

    pipe(topology_base &topology, int32_t partition)
            : event_consumer<K, void>(), partition_source<K, void>(nullptr, partition) {
    }

    pipe(topology_base &topology, std::shared_ptr<kspp::partition_source<K, void>> upstream)
            : event_consumer<K, void>(), partition_source<K, void>(upstream.get(), upstream->partition()) {
      if (upstream)
        upstream->add_sink([this](std::shared_ptr<kevent<K, void>> r) {
          this->send_to_sinks(r);
        });
    }

    pipe(topology_base &topology, std::vector<std::shared_ptr<kspp::partition_source<K, void>>> upstream, int32_t partition)
            : event_consumer<K, void>()
              , partition_source<K, void>(nullptr, partition) {
      for (auto i : upstream) {
        this->add_upstream(i.get());
        i->add_sink([this](auto r) {
          this->send_to_sinks(r);
        });
      }
    }

    std::string simple_name() const override {
      return "pipe";
    }

    bool process_one(int64_t tick) override {
      bool processed = false;
      for (auto i : this->upstream_)
        processed = i->process_one(tick);
      return processed;
    }


    size_t queue_len() const override {
      return event_consumer<K, void>::queue_len();
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
};