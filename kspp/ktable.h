#include <boost/filesystem.hpp>
#include "kstream.h"
#pragma once

namespace csi {

  template<class K, class V, class codec>
  class ktable : public kstream<K, V, codec>
  {
  public:
    ktable(std::string nodeid, std::string brokers, std::string topic, int32_t partition, std::string storage_path, std::shared_ptr<codec> codec) :
      kstream(nodeid, brokers, topic, partition, storage_path, codec) {}
  };
};


  //class ktable
//{
//  public:
//  ktable(std::string brokers, std::string topic, int32_t partition, std::string root_path);
//  ~ktable();
//  void close();
//  std::unique_ptr<RdKafka::Message> consume();
//  inline bool eof() const {
//    return _consumer.eof();
//  }
//  std::unique_ptr<RdKafka::Message> find(const void* key, size_t key_size) {
//    return _local_storage.get(key, key_size);
//  }
//
//  void flush_offset();
//
//  private:
//  boost::filesystem::path _storage_path;
//  boost::filesystem::path _offset_storage_path;
//  kafka_consumer          _consumer;
//  kafka_local_store       _local_storage;
//  int64_t                 _last_offset;
//  int64_t                 _last_flushed_offset;
//};
//
//template<class K, class V, class codec>
//class ktable2
//{
//  public:
//  struct item_type
//  {
//    item_type() : event_time(0) {}
//    K                  key;
//    std::unique_ptr<V> value;
//    int64_t            event_time;
//  };
//
//  //typedef std::function<uint32_t(const K& key, const V& val)> partitioner;
//
//  ktable2(std::string brokers, std::string topic, int32_t partition, std::string storage_path, std::shared_ptr<codec> codec) : _impl(brokers, topic, partition, storage_path), _codec(codec) {}
//  ~ktable2() {}
//  inline void close() { _impl.close(); }
//
//  inline bool eof() const {
//    return _impl.eof();
//  }
//
//  inline void flush_offset() {
//    _impl.flush_offset();
//  }
//
//  std::unique_ptr<RdKafka::Message> consume() {
//    return _impl.consume();
//  }
//
//  std::unique_ptr<RdKafka::Message> find(const void* key, size_t key_size) {
//    return _impl.find(key, key_size);
//  }
//
//  std::unique_ptr<item_type> parse(const std::unique_ptr<RdKafka::Message> & ref) {
//    if (!ref)
//      return NULL;
//
//    std::unique_ptr<item_type> res(new item_type);
//
//    {
//      std::istrstream ks((const char*) ref->key_pointer(), ref->key_len());
//      if (_codec->decode(ks, res->key) == 0) {
//        std::cerr << "ktable2::parse, decode key failed" << std::endl;
//        return NULL;
//      }
//    }
//
//    size_t sz = ref->len();
//    if (sz) {
//      std::istrstream vs((const char*) ref->payload(), sz);
//      res->value = std::unique_ptr<V>(new V);
//      size_t consumed = _codec->decode(vs, *res->value);
//      if (consumed == 0) {
//        std::cerr << "ktable2::parse, decode value failed" << std::endl;
//        return NULL;
//      }
//      assert(consumed == sz);
//    }
//    return res;
//  }
//
//  /*
//  template<class KSTREAM>
//  void join(KSTREAM& stream)  {
//    while (run) {
//      auto ev = stream.consume();
//      if (ev) {
//        auto row = find(ev->key_pointer(), ev->key_len());
//        if (row) {
//          auto e = stream.parse(ev);
//          auto r = parse(row);
//          //e->value->
//        }
//      }
//      if (event_source.eof())
//        break;
//    }
//  }
//  */
//        
//  private:
//  ktable                 _impl;
//  std::shared_ptr<codec> _codec;
//  //std::function<>        _on_row;
//};

//}; // namespace