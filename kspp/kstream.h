#include "kafka_consumer.h"
#include "kafka_local_store.h"
#pragma once

namespace csi {
class kstream
{
  public:
  kstream(std::string brokers, std::string topic, int32_t partition, std::string storage_path);
  ~kstream();
  void close();

  std::unique_ptr<RdKafka::Message> consume();
  inline bool eof() const {
    return _consumer.eof();
  }
  private:
  kafka_consumer    _consumer;
  kafka_local_store _local_storage;
};

template<class K, class V, class codec>
class kstream2
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

  kstream2(std::string brokers, std::string topic, int32_t partition, std::string storage_path, std::shared_ptr<codec> codec) : _impl(brokers, topic, partition, storage_path), _codec(codec) {}
  ~kstream2() {}

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

  std::unique_ptr<item_type> parse(const std::unique_ptr<RdKafka::Message>& ref) {
    if (!ref)
      return NULL;

    std::unique_ptr<item_type> res(new item_type);

    {
      std::istrstream ks((const char*)ref->key_pointer(), ref->key_len());
      if (_codec->decode(ks, res->key) == 0)
      {
        std::cerr << "kstream2::parse, decode key failed" << std::endl;
        return NULL;
      }
    }

    // NULL is ok in stream but not in storage???
    size_t sz = ref->len();
    if (sz)
    {
      std::istrstream vs((const char*) ref->payload(), sz);
      res->value = std::unique_ptr<V>(new V);
      size_t consumed = _codec->decode(vs, *res->value);
      if (consumed==0)
      {
        std::cerr << "kstream2::parse, decode value failed" << std::endl;
        return NULL;
      }
      assert(consumed == sz);
    }
    return res;
  }

private:
  kstream                _impl;
  std::shared_ptr<codec> _codec;
};


};
