#include <memory>
#include <librdkafka/rdkafkacpp.h>
#pragma once

namespace csi {
class kafka_producer
{
  public:
  enum rdkafka_memory_management_mode { NO_COPY = 0, FREE = 1, COPY = 2 };

  kafka_producer(std::string brokers, std::string topic);
  ~kafka_producer();

  int produce(int32_t partition, rdkafka_memory_management_mode mode, void* key, size_t keysz, void* value, size_t valuesz);
  void close();
  
  inline std::string topic() const { 
    return _topic; 
  }

  inline size_t queue_len() const { 
    return _producer->outq_len(); 
  }

  inline void poll(int timeout) {
    _producer->poll(timeout);
  }

  private:
  const std::string                  _topic;
  std::unique_ptr<RdKafka::Topic>    _rd_topic;
  std::unique_ptr<RdKafka::Producer> _producer;
  uint64_t                           _msg_cnt;
  uint64_t                           _msg_bytes;
};
}; // namespace


