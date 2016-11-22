#pragma once
#include <memory>
#include  <librdkafka/rdkafkacpp.h>

#pragma once

namespace csi {
class kafka_producer
{
  public:
  enum rdkafka_memory_management_mode { NO_COPY=0, FREE=1, COPY=2 };

  kafka_producer(std::string brokers, std::string topic);
  ~kafka_producer();
  int produce(int32_t partition, rdkafka_memory_management_mode mode, void* key, size_t keysz, void* value, size_t valuesz);
  void close();
  int32_t nr_of_partitions() const;
  private:
  const std::string  _topic;
  RdKafka::Topic*    _rd_topic;
  RdKafka::Producer* _producer;
 
  int32_t  _nr_of_partitions;
  uint64_t _msg_cnt;
  uint64_t _msg_bytes;
};
}; // namespace
