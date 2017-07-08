#include <fstream>
#include <boost/filesystem.hpp>
#include <kspp/kspp.h>
#pragma once

namespace kspp {
  template<class K, class V, template <typename, typename, typename> class STATE_STORE, class CODEC=void>
  class ktable : public event_consumer<K, V>, public materialized_source<K, V>
  {
  public:
    template<typename... Args>
    ktable(topology_base& topology, std::shared_ptr<kspp::partition_source<K, V>> source, Args... args)
      : event_consumer<K, V>()
      , materialized_source<K, V>(source.get(), source->partition())
      , _source(source), _state_store(this->get_storage_path(topology.get_storage_path()), args...),
        _in_count("in_count"),
        _state_store_count("state_store_count", [this]() { return _state_store.aprox_size(); }) {
      _source->add_sink([this](auto ev) {
        _lag.add_event_time(kspp::milliseconds_since_epoch(), ev->event_time());
        ++_in_count;
        _state_store.insert(ev->record(), ev->offset());
        this->send_to_sinks(ev);
      });
      // what to do with state_store deleted records (windowed)
      _state_store.set_sink([this](auto ev) {
        this->send_to_sinks(ev);
      });
      this->add_metric(&_lag);
      this->add_metric(&_in_count);
      this->add_metric(&_state_store_count);
    }

    virtual ~ktable() {
      close();
    }

    virtual std::string simple_name() const {
      return "ktable";
    }

    /*
    virtual std::string full_name() const {
      return "ktable<" + STATE_STORE<K, V, CODEC>::type_name() + ">";
    }
    */

    virtual void start() {
      _source->start(_state_store.offset());
    }

    virtual void start(int64_t offset) {
      _state_store.start(offset);
      _source->start(offset);
    }

    virtual void commit(bool flush) {
      _state_store.commit(flush);
    }

    virtual void close() {
      _source->close();
      _state_store.close();
    }

    virtual bool eof() const {
      return _source->eof();
    }

    virtual bool process_one(int64_t tick) {
      return _source->process_one(tick);
    }

    virtual void garbage_collect(int64_t tick) {
      _state_store.garbage_collect(tick);
    }

    virtual int64_t offset() const {
      return _state_store.offset();
    }

    virtual size_t queue_len() const {
      return event_consumer<K, V>::queue_len();
    }

    virtual std::shared_ptr<const krecord <K, V>> get(const K &key) const {
      return _state_store.get(key);
    }

    virtual typename kspp::materialized_source<K, V>::iterator begin(void) const {
      return _state_store.begin();
    }

    virtual typename kspp::materialized_source<K, V>::iterator end() const {
      return _state_store.end();
    }

  private:
    std::shared_ptr<kspp::partition_source<K, V>> _source;
    STATE_STORE<K, V, CODEC>                      _state_store;
    metric_lag                                    _lag;
    metric_counter                                _in_count;
    metric_evaluator                              _state_store_count;
  };
};
