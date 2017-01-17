#include <memory>
#include <librdkafka/rdkafkacpp.h>
#pragma once

namespace kspp {
  class kafka_producer
  {
  public:
    enum rdkafka_memory_management_mode { NO_COPY = 0, FREE = 1, COPY = 2 };

    kafka_producer(std::string brokers, std::string topic);
    ~kafka_producer();

    /**
    produce a message to partition -> (partition_hash % partition_cnt)
    */
    int produce(uint32_t partition_hash, rdkafka_memory_management_mode mode, void* key, size_t keysz, void* value, size_t valuesz);
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
    class MyHashPartitionerCb : public RdKafka::PartitionerCb {
    public:
      int32_t partitioner_cb(const RdKafka::Topic *topic, const std::string *key, int32_t partition_cnt, void *msg_opaque);
    };

    const std::string                  _topic;
    std::unique_ptr<RdKafka::Topic>    _rd_topic;
    std::unique_ptr<RdKafka::Producer> _producer;
    bool                               _closed;
    uint64_t                           _msg_cnt;
    uint64_t                           _msg_bytes;
    MyHashPartitionerCb                _default_partitioner;
  };
}; // namespace


