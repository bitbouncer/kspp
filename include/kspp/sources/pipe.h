#include <kspp/kspp.h>
#pragma once

//TODO rename to mem_stream ???
//add ordering of events in queue
//change upstream lambda to insert in queue

namespace kspp {
  template<class K, class V>
  class pipe : public event_consumer<K, V>, public partition_source<K, V> {
  public:
    typedef K key_type;
    typedef V value_type;
    typedef kspp::kevent<K, V> record_type;

    pipe(topology &t, int32_t partition)
        : event_consumer<K, V>()
        , partition_source<K, V>(nullptr, partition) {
    }

    pipe(topology &topology, std::shared_ptr<kspp::partition_source<K, V>> upstream)
        : event_consumer<K, V>()
        , partition_source<K, V>(upstream.get(), upstream->partition()) {
      upstream->add_sink([this](auto e) {
        this->_queue.push_back(e);
      });
    }

    pipe(topology &t, std::vector<std::shared_ptr<kspp::partition_source<K, V>>> upstream, int32_t partition)
        : event_consumer<K, V>()
        , partition_source<K, V>(nullptr, partition) {
      for (auto i : upstream) {
        this->add_upstream(i.get());
        i->add_sink([this](auto e) {
          this->_queue.push_back(e);
        });
      }
    }

    std::string simple_name() const override {
      return "pipe";
    }

    size_t process(int64_t tick) override {
      for (auto i : this->upstream_)
        i->process(tick);

      size_t processed=0;
      //forward up this timestamp
      while (this->_queue.next_event_time()<=tick){
        auto p = this->_queue.pop_and_get();
        this->send_to_sinks(p);
        ++processed;
      }
    }

    size_t queue_size() const override {
      return event_consumer<K, V>::queue_size();
    }

    void commit(bool force) override {
      for (auto i : this->upstream_)
        i->commit(force);
    }
  };

//<null, VALUE>
  template<class V>
  class pipe<void, V> : public event_consumer<void, V>, public partition_source<void, V> {
  public:
    typedef void key_type;
    typedef V value_type;
    typedef kspp::kevent<void, V> record_type;

    pipe(topology &t, int32_t partition)
        : event_consumer<void, V>()
        , partition_source<void, V>(nullptr, partition) {
    }

    pipe(topology &t, std::shared_ptr<kspp::partition_source<void, V>> upstream)
        : event_consumer<void, V>(), partition_source<void, V>(upstream.get(), upstream->partition()) {
      if (upstream)
        upstream->add_sink([this](auto e) {
          this->_queue.push_back(e);
        });
    }

    pipe(topology &t, std::vector<std::shared_ptr<kspp::partition_source<void, V>>> upstream, int32_t partition)
        : event_consumer<void, V>()
        , partition_source<void, V>(nullptr, partition) {
      for (auto i : upstream) {
        this->add_upstream(i.get());
        i->add_sink([this](auto e) {
          this->_queue.push_back(e);
        });
      }
    }

    std::string simple_name() const override {
      return "pipe";
    }

    size_t process(int64_t tick) override {
      for (auto i : this->upstream_)
        i->process(tick);

      size_t processed=0;

      //forward up this timestamp
      while (this->_queue.next_event_time()<=tick){
        auto p = this->_queue.pop_and_get();
        this->send_to_sinks(p);
        ++processed;
      }
      return processed;
    }

    size_t queue_size() const override {
      return event_consumer<void, V>::queue_size();
    }

    void commit(bool force) override {
      for (auto i : this->upstream_)
        i->commit(force);
    }
  };

  template<class K>
  class pipe<K, void> : public event_consumer<K, void>, public partition_source<K, void> {
  public:
    typedef K key_type;
    typedef void value_type;
    typedef kspp::kevent<K, void> record_type;

    pipe(topology &t, int32_t partition)
        : event_consumer<K, void>()
        , partition_source<K, void>(nullptr, partition) {
    }

    pipe(topology &t, std::shared_ptr<kspp::partition_source<K, void>> upstream)
        : event_consumer<K, void>()
        , partition_source<K, void>(upstream.get(), upstream->partition()) {
      if (upstream)
        upstream->add_sink([this](std::shared_ptr<kevent<K, void>> e) {
          this->_queue.push_back(e);
        });
    }

    pipe(topology &t, std::vector<std::shared_ptr<kspp::partition_source<K, void>>> upstream, int32_t partition)
        : event_consumer<K, void>()
        , partition_source<K, void>(nullptr, partition) {
      for (auto i : upstream) {
        this->add_upstream(i.get());
        i->add_sink([this](auto e) {
          this->_queue.push_back(e);
        });
      }
    }

    std::string simple_name() const override {
      return "pipe";
    }

    size_t process(int64_t tick) override {
      for (auto i : this->upstream_)
        i->process(tick);

      size_t processed=0;
      //forward up this timestamp
      while (this->_queue.next_event_time()<=tick){
        auto p = this->_queue.pop_and_get();
        this->send_to_sinks(p);
        ++processed;
      }
      return processed;
    }


    size_t queue_size() const override {
      return event_consumer<K, void>::queue_size();
    }

    void commit(bool force) override {
      for (auto i : this->upstream_)
        i->commit(force);
    }
  };
}