#pragma once
#include <memory>
#include  <librdkafka/rdkafkacpp.h>
#include <rocksdb/db.h>

#pragma once

namespace csi {

struct uuid
{
  char data[16];
};

struct rocksdb_inner_key
{
  uuid id;
  uint32_t ts;
  int64_t index;
};

/*
namespace WindowStoreUtils {
void toBinaryKey(const uuid* id, uint32_t ts,  int64_t index, rocksdb_inner_key* ik);
}
*/

class kafka_consumer
{
  public:
  kafka_consumer(std::string brokers, std::string topic, int32_t partition);
  ~kafka_consumer();
  std::unique_ptr<RdKafka::Message> consume();
  inline bool eof() const {
    return _eof; 
  }
  private:
  const std::string  _topic;
  const int32_t      _partition;
  RdKafka::Topic*    _rd_topic;
  RdKafka::Consumer* _consumer;

  uint64_t _msg_cnt;
  uint64_t _msg_bytes;
  bool     _eof;
};

class rockdb_impl
{
  public:
  rockdb_impl(std::string storage_path);
  ~rockdb_impl();
  void put(RdKafka::Message*);
  std::unique_ptr<RdKafka::Message> get(const void* key, size_t key_size);
  private:
  rocksdb::DB* _db;
};
}

