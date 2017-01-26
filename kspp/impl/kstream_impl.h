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
    kstream_partition_impl(partition_topology_base& topology, std::shared_ptr<kspp::partition_source<K, V>> source, std::shared_ptr<CODEC> codec)
      : kstream_partition<K, V>(source.get())
      , _offset_storage_path(get_storage_path(topology.get_storage_path()))
      , _source(source)
      , _state_store(get_storage_path(topology.get_storage_path()), codec)
      , _current_offset(RdKafka::Topic::OFFSET_BEGINNING)
      , _last_comitted_offset(RdKafka::Topic::OFFSET_BEGINNING)
      , _last_flushed_offset(RdKafka::Topic::OFFSET_BEGINNING) {
      boost::filesystem::create_directories(_offset_storage_path);
      _offset_storage_path /= "kspp_offset.bin";

      // this is probably wrong since why should we delete the row in a kstream??? TBD
      // for now this is just a copy from ktable...
      _source->add_sink([this](auto r) {
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

    virtual std::string name() const {
      return   _source->name() + "-kstream";
    }

    virtual std::string processor_name() const { return "kstream"; }

    virtual void start() {
      if (boost::filesystem::exists(_offset_storage_path)) {
        std::ifstream is(_offset_storage_path.generic_string(), std::ios::binary);
        int64_t tmp;
        is.read((char*)&tmp, sizeof(int64_t));
        if (is.good()) {
          _current_offset = tmp;
          _last_comitted_offset = tmp;
          _last_flushed_offset = tmp;
        }
      }
      _source->start(_current_offset);
    }

    virtual void start(int64_t offset) {
      _current_offset = offset;
      _source->start(_current_offset);
    }

    virtual void commit() {
      _last_comitted_offset = _current_offset;
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
      if (_last_flushed_offset != _last_comitted_offset) {
        std::ofstream os(_offset_storage_path.generic_string(), std::ios::binary);
        os.write((char*)&_last_comitted_offset, sizeof(int64_t));
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

  private:
  boost::filesystem::path get_storage_path(boost::filesystem::path storage_path) {
    boost::filesystem::path p(storage_path);
    p /= name();
    return p;
  }

  std::shared_ptr<kspp::partition_source<K, V>> _source;
  rockdb_store<K, V, CODEC>                     _state_store;
  boost::filesystem::path                       _offset_storage_path;
  int64_t                                       _current_offset;
  int64_t                                       _last_comitted_offset;
  int64_t                                       _last_flushed_offset;
  };
};
