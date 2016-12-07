#pragma once
#include <memory>
#include <boost/filesystem.hpp>
#include <rocksdb/db.h>
#include  <librdkafka/rdkafkacpp.h>

namespace csi {


/*
namespace WindowStoreUtils {
void toBinaryKey(const uuid* id, uint32_t ts,  int64_t index, rocksdb_inner_key* ik);
}
*/

class kafka_local_store
{
  public:
  kafka_local_store(boost::filesystem::path storage_path);
  ~kafka_local_store();
  void close();
  void put(RdKafka::Message*);
  std::unique_ptr<RdKafka::Message> get(const void* key, size_t key_size);
  private:
  rocksdb::DB* _db;
};
}

