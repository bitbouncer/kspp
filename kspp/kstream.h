#include "kafka_consumer.h"
#include "kafka_local_store.h"
#pragma once

namespace csi {
class kstream
{
  public:
  kstream(std::string brokers, std::string topic, int32_t partition, std::string storage_path);
  ~kstream();
  std::unique_ptr<RdKafka::Message> consume();
  inline bool eof() const {
    return _consumer.eof();
  }
  private:
  kafka_consumer    _consumer;
  kafka_local_store _local_storage;
};
};
