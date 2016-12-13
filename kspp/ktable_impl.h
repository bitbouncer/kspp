#include <boost/filesystem.hpp>
#include "kafka_source.h"
#include "state_store.h"
#include "kspp_defs.h"
#pragma once

#include "kafka_source.h"
#include "state_store.h"
#pragma once

namespace csi {
template<class K, class V, class codec>
class ktable_impl : public ktable<K, V>
{
  public:
  ktable_impl(std::string nodeid, std::string brokers, std::string topic, int32_t partition, std::string storage_path, std::shared_ptr<codec> codec) :
    _offset_storage_path(storage_path),
    _source(brokers, topic, partition, codec),
    _state_store(topic, partition, storage_path + "\\" + nodeid + "\\" + topic + "_" + std::to_string(partition), codec),
    _current_offset(RdKafka::Topic::OFFSET_BEGINNING),
    _last_comitted_offset(RdKafka::Topic::OFFSET_BEGINNING),
    _last_flushed_offset(RdKafka::Topic::OFFSET_BEGINNING) {
    _offset_storage_path /= nodeid;
    _offset_storage_path /= topic + "_" + std::to_string(partition);
    boost::filesystem::create_directories(_offset_storage_path);
    _offset_storage_path /= "\\kafka_offset.bin";
  }

  virtual ~ktable_impl() {
    close();
  }

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

  virtual  std::shared_ptr<krecord<K, V>> consume() {
    auto p = _source.consume();
    if (p) {
      _current_offset = p->offset;
      if (p->value)
        _state_store.put(p->key, *p->value);
      else
        _state_store.del(p->key);
    }
    return p;
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

  virtual std::shared_ptr<krecord<K, V>> get(const K& key) {
    return _state_store.get(key);
  }

  /*
  virtual std::shared_ptr<csi::ktable_iterator<K, V>> iteratorX() {
    return _state_store.iterator();
  }
  */

  virtual csi::ktable<K, V>::iterator begin(void) {
    return _state_store.begin();
  }

  virtual csi::ktable<K, V>::iterator end() {
    return _state_store.end();
  }

  /*
  virtual ktable_range_iterator<K, V> begin() {
    return _state_store.begin();
  }

  virtual ktable_range_iterator<K, V> end() {
    return _state_store.end();
  }
  */

  private:
  kafka_source<K, V, codec> _source;
  kstate_store<K, V, codec> _state_store;
  boost::filesystem::path   _offset_storage_path;
  int64_t                   _current_offset;
  int64_t                   _last_comitted_offset;
  int64_t                   _last_flushed_offset;
};
};
