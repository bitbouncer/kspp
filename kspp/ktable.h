#include "kspp.h"
#pragma once

namespace csi {
class ktable
{
  public:
  ktable(std::string brokers, std::string topic, int32_t partition, std::string storage_path);
  ~ktable();
  std::unique_ptr<RdKafka::Message> consume();
  inline bool eof() const {
    return _consumer.eof();
  }
  std::unique_ptr<RdKafka::Message> find(const void* key, size_t key_size) {
    return _local_storage.get(key, key_size);
  }
  private:
  kafka_consumer _consumer;
  rockdb_impl    _local_storage;
};
};
