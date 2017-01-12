#include <fstream>
#include <boost/filesystem.hpp>
#include <kspp/sources/kafka_source.h>
#include <kspp/state_stores/rocksdb_store.h>
#include <kspp/kspp.h>
#pragma once

namespace kspp {
template<class K, class V, class CODEC>
class kstream_partition_impl : public kstream_partition<K, V>
{
  public:
  kstream_partition_impl(std::string nodeid, std::string brokers, std::string topic, size_t partition, std::string root_storage_path, std::shared_ptr<CODEC> codec)
    : kstream_partition<K, V>(NULL, partition)
    , _offset_storage_path(get_storage_path(root_storage_path, nodeid, topic, partition))
    , _source(brokers, topic, partition, codec)
    , _state_store(topic, partition, get_storage_path(root_storage_path, nodeid, topic, partition), codec)
    , _current_offset(RdKafka::Topic::OFFSET_BEGINNING)
    , _last_comitted_offset(RdKafka::Topic::OFFSET_BEGINNING)
    , _last_flushed_offset(RdKafka::Topic::OFFSET_BEGINNING) {
    boost::filesystem::create_directories(_offset_storage_path);
    _offset_storage_path /= "\\kafka_offset.bin";

    // this is probably wrong since why should we delete the row in a kstream??? TBD
    // for now this is just a copy from ktable...
    _source.add_sink([this](auto r) {
      _current_offset = r->offset;
      if (r->value)
        _state_store.put(r->key, *r->value);
      else
        _state_store.del(r->key);
      this->send_to_sinks(r);
    });

  }

  virtual ~kstream_partition_impl() {
    close();
  }

  //std::string name() const { return "kstream_partition_impl_" + _source.topic() + "_" + std::to_string(partition()); }
  std::string name() const { return "kstream_partition_impl_" + std::to_string(this->partition()); }

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

  virtual bool is_dirty() {
    return _source.is_dirty();
  }

  inline int64_t offset() const {
    return _current_offset;
  }

  virtual std::shared_ptr<krecord<K, V>> get(const K& key) {
    return _state_store.get(key);
  }

  private:
  static boost::filesystem::path get_storage_path(std::string storage_path, std::string node_id, std::string topic, size_t partition) {
    boost::filesystem::path p(storage_path);
    p /= node_id;
    p /= topic + std::to_string(partition);
    return p;
  }

  kafka_source<K, V, CODEC> _source; // TBD this should be a stream-source....
  rockdb_store<K, V, CODEC> _state_store;

  boost::filesystem::path _offset_storage_path;
  int64_t                 _current_offset;
  int64_t                 _last_comitted_offset;
  int64_t                 _last_flushed_offset;
};
};
