#include "kafka_consumer.h"
#include "kafka_local_store.h"
#include <strstream>
#pragma once

namespace csi {
  class ktable
  {
  public:
    ktable(std::string brokers, std::string topic, int32_t partition, std::string storage_path);
    ~ktable();
    void close();
    std::unique_ptr<RdKafka::Message> consume();
    inline bool eof() const {
      return _consumer.eof();
    }
    std::unique_ptr<RdKafka::Message> find(const void* key, size_t key_size) {
      return _local_storage.get(key, key_size);
    }
  private:
    kafka_consumer    _consumer;
    kafka_local_store _local_storage;
  };


  template<class K, class V, class codec>
  class ktable2
  {
  public:
    struct item_type
    {
      item_type() : event_time(0) {}
      K                  key;
      std::unique_ptr<V> value;
      int64_t            event_time;
    };

    //typedef std::function<uint32_t(const K& key, const V& val)> partitioner;

    ktable2(std::string brokers, std::string topic, int32_t partition, std::string storage_path, std::shared_ptr<codec> codec) : _impl(brokers, topic, partition, storage_path), _codec(codec) {}
    ~ktable2() {}
    inline void close() { _impl.close(); }

    inline bool eof() const {
      return _impl.eof();
    }

    std::unique_ptr<RdKafka::Message> consume() {
      return _impl.consume();
    }

    std::unique_ptr<RdKafka::Message> find(const void* key, size_t key_size) {
      return _impl.find(key, key_size);
    }

    std::unique_ptr<item_type> parse(const std::unique_ptr<RdKafka::Message> & ref) {
      if (!ref)
        return NULL;

      std::unique_ptr<item_type> res(new item_type);

      {
        std::istrstream ks((const char*)ref->key_pointer(), ref->key_len());
        if (_codec->decode(ks, res->key) == 0)
        {
          std::cerr << "ktable2::parse, decode key failed" << std::endl;
          return NULL;
        }
      }

      size_t sz = ref->len();
      if (sz)
      {
        std::istrstream vs((const char*) ref->payload(), sz);
        res->value = std::unique_ptr<V>(new V);
        size_t consumed = _codec->decode(vs, *res->value);
        if (consumed == 0)
        {
          std::cerr << "ktable2::parse, decode value failed" << std::endl;
          return NULL;
        }
        assert(consumed == sz);
      }
      return res;
    }

    //std::unique_ptr<V> find(const K& key) {
    //  char buf[1024];
    //  std::strstream ks(buf, 1024);
    //  ksize = _codec->encode(key, ks);
    //  ks.read(buf, ksize);
    //  auto p = _impl.get(buf, ksize);
    //  if (!p)
    //    return NULL;




    //  std::strstream vs(p->payload(), p->len());
    //  std::unique_ptr<V> vp(new V);
    //  if (_coded->decode(vs, *vp))
    //  {
    //    return vp;
    //    /*
    //    std::unique_ptr<item_type> result(new item_type);
    //    result->key = key;
    //    result->value = vp;
    //    */
    //  }

    //  std::cerr << "decode failed" << std::endl;
    //  return NULL;
    //}


  private:
    ktable _impl;
    std::shared_ptr<codec> _codec;
  };

}; // namespace