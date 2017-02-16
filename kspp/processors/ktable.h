#include <fstream>
#include <boost/filesystem.hpp>
#include <kspp/kspp.h>
#pragma once

namespace kspp {
  template<class K, class V, template <typename, typename, typename> class STATE_STORE, class CODEC=void>
  class ktable : public materialized_source<K, V>
  {
  public:
    template<typename... Args>
    ktable(topology_base& topology, std::shared_ptr<kspp::partition_source<K, V>> source, Args... args)
      : materialized_source<K, V>(source.get(), source->partition())
      , _source(source)
      , _state_store(get_storage_path(topology.get_storage_path()), args...)
      , _in_count("in_count")
      , _state_store_count("state_store_count", [this]() { return _state_store.size(); }) {
      _source->add_sink([this](auto r) {
        _lag.add_event_time(r->event_time);
        ++_in_count;
        _state_store.insert(r);
        this->send_to_sinks(r);
      });
      this->add_metric(&_lag);
      this->add_metric(&_in_count);
      this->add_metric(&_state_store_count);
    }

    virtual ~ktable() {
      close();
    }

    virtual std::string name() const {
      return   _source->name() + "-ktable<" + STATE_STORE<K, V, CODEC>::type_name() + ">";
    }

    virtual std::string processor_name() const { 
      return "ktable<" + STATE_STORE<K, V, CODEC>::type_name() + ">";
    }

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

    virtual bool process_one() {
      return _source->process_one();
    }

    virtual int64_t offset() const {
      return _state_store.offset();
    }

    virtual std::shared_ptr<krecord<K, V>> get(const K& key) {
      return _state_store.get(key);
    }

    virtual typename kspp::materialized_source<K, V>::iterator begin(void) {
      return _state_store.begin();
    }

    virtual typename kspp::materialized_source<K, V>::iterator end() {
      return _state_store.end();
    }

  private:
    boost::filesystem::path get_storage_path(boost::filesystem::path storage_path) {
      boost::filesystem::path p(storage_path);
      p /= sanitize_filename(name());
      return p;
    }

    std::shared_ptr<kspp::partition_source<K, V>> _source;
    STATE_STORE<K, V, CODEC>                      _state_store;
    metric_lag                                    _lag;
    metric_counter                                _in_count;
    metric_evaluator                              _state_store_count;
  };
};
