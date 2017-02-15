#include <fstream>
#include <boost/filesystem.hpp>
#include <kspp/impl/state_stores/rocksdb_store.h>
#include <kspp/kspp.h>
#pragma once

namespace kspp {
template<class K, class V, class CODEC>
class ktable_rocksdb : public ktable<K, V>
{
  public:
  ktable_rocksdb(topology_base& topology, std::shared_ptr<kspp::partition_source<K, V>> source, std::shared_ptr<CODEC> codec)
    : ktable<K, V>(source.get())
    , _source(source)
    , _state_store(get_storage_path(topology.get_storage_path()), codec)
    , _count("count") {
    _source->add_sink([this](auto r) {
      _lag.add_event_time(r->event_time);
      ++_count;
      _state_store.insert(r);
      this->send_to_sinks(r);
    });
    this->add_metric(&_lag);
    this->add_metric(&_count);
  }

  virtual ~ktable_rocksdb() {
    close();
  }

  virtual std::string name() const {
    return   _source->name() + "-ktable_rocksb";
  }

  virtual std::string processor_name() const { return "ktable_rocksdb"; }

  virtual void start() {
    _source->start(_state_store.offset());
  }

  virtual void start(int64_t offset) {
    _state_store.start(offset);
    _source->start(offset);
  }

  virtual void commit() {
    _state_store.commit();
  }

  virtual void close() {
    _source->close();
    _state_store.close();
    flush_offset();
  }

  virtual bool eof() const {
    return _source->eof();
  }

  virtual bool process_one() {
    return _source->process_one();
  }

  virtual void flush_offset() {
    _state_store.commit();
    _state_store.flush_offset();
  }

  inline int64_t offset() const {
    return _current_offset;
  }

  // inherited from kmaterialized_source
  virtual std::shared_ptr<krecord<K, V>> get(const K& key) {
    return _state_store.get(key);
  }

  virtual typename kspp::materialized_partition_source<K, V>::iterator begin(void) {
    return _state_store.begin();
  }

  virtual typename kspp::materialized_partition_source<K, V>::iterator end() {
    return _state_store.end();
  }

  private:
  boost::filesystem::path get_storage_path(boost::filesystem::path storage_path) {
    boost::filesystem::path p(storage_path);
    p /= sanitize_filename(name());
    return p;
  }

  std::shared_ptr<kspp::partition_source<K, V>> _source;
  rockdb_store<K, V, CODEC>                     _state_store;
  metric_lag                                    _lag;
  metric_counter                                _count;
};
};
