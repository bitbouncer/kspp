#include <boost/filesystem.hpp>
#include <kspp/state_stores/rocksdb_store.h>
#include <kspp/sources/kafka_source.h>
#include <kspp/kspp.h>
#pragma once

namespace kspp {
template<class K, class V, class CODEC>
class ktable_partition_impl : public ktable_partition<K, V>
{
  public:
  ktable_partition_impl(std::string brokers, std::string topic, size_t partition, boost::filesystem::path storage_path, std::shared_ptr<CODEC> codec)
    : ktable_partition<K, V>(NULL, partition)
    , _offset_storage_path(get_storage_path(storage_path))
    , _source(brokers, topic, partition, codec)
    , _state_store(topic, partition, get_storage_path(storage_path), codec)
    , _current_offset(RdKafka::Topic::OFFSET_BEGINNING)
    , _last_comitted_offset(RdKafka::Topic::OFFSET_BEGINNING)
    , _last_flushed_offset(RdKafka::Topic::OFFSET_BEGINNING)
    , _count("count") {
    boost::filesystem::create_directories(_offset_storage_path);
    _offset_storage_path /= "kspp_offset.bin";
    _source.add_sink([this](auto r) {
      _current_offset = r->offset;
      _lag.add_event_time(r->event_time);
      ++_count;
      if (r->value)
        _state_store.put(r->key, *r->value);
      else
        _state_store.del(r->key);
      this->send_to_sinks(r);
    });
    this->add_metric(&_lag);
    this->add_metric(&_count);
  }

  virtual ~ktable_partition_impl() {
    close();
  }

  virtual std::string name() const {
    return   _source.name() + "-ktable";
  }

  virtual std::string processor_name() const { return "ktable"; }

  virtual void start() {
    if (boost::filesystem::exists(_offset_storage_path)) {
      std::ifstream is(_offset_storage_path.generic_string(), std::ios::binary);
      int64_t tmp;
      is.read((char*) &tmp, sizeof(int64_t));
      if (is.good()) {
        _current_offset = tmp;
        _last_comitted_offset = tmp;
        _last_flushed_offset = tmp;
      }
    }
    _source.start(_current_offset);
  }

  virtual void start(int64_t offset) {
    _current_offset = offset;
    _source.start(_current_offset);
  }

  virtual void commit() {
    _last_comitted_offset = _current_offset;
  }

  virtual void close() {
    _source.close();
    _state_store.close();
    flush_offset();
  }

  virtual bool eof() const {
    return _source.eof();
  }

  virtual bool is_dirty() {
    return _source.is_dirty();
  }

  virtual bool process_one() {
    return _source.process_one();
  }

  virtual void flush_offset() {
    if (_last_flushed_offset != _last_comitted_offset) {
      std::ofstream os(_offset_storage_path.generic_string(), std::ios::binary);
      os.write((char*) &_last_comitted_offset, sizeof(int64_t));
      _last_flushed_offset = _last_comitted_offset;
      os.flush();
    }
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
    p /= name();
    return p;
  }

  kafka_source<K, V, CODEC> _source;  // TBD this should be a stream-source....
  rockdb_store<K, V, CODEC> _state_store;
  boost::filesystem::path   _offset_storage_path;
  int64_t                   _current_offset;
  int64_t                   _last_comitted_offset;
  int64_t                   _last_flushed_offset;
  metric_lag                _lag;
  metric_counter            _count;
};
};
