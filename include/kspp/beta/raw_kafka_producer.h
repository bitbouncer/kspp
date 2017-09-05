#include <memory>
#include <string>
#include <cstdint>
#include <map>
#include <librdkafka/rdkafkacpp.h>
#include <kspp/kspp.h>
#pragma once

namespace kspp {
class raw_kafka_producer
{
  public:
  enum memory_management_mode { FREE = 1, COPY = 2 };

  raw_kafka_producer(std::shared_ptr<cluster_config> config, std::string topic);

  ~raw_kafka_producer();

  void close();

  /**
  produce a message to partition -> (partition_hash % partition_cnt)
  */
  int produce(uint32_t partition_hash, memory_management_mode mode, void* key, size_t keysz, void* value, size_t valuesz, int64_t timestamp, std::shared_ptr<commit_chain::autocommit_marker> autocommit_marker);

  inline std::string topic() const {
    return _topic;
  }

  inline size_t queue_len() const {
    return _producer->outq_len();
  }

  inline void poll(int timeout) {
    _producer->poll(timeout);
  }

  inline bool good() const {
    return (_delivery_report_cb.status() == RdKafka::ErrorCode::ERR_NO_ERROR);
  }

  inline size_t nr_of_partitions() {
    return _nr_of_partitions;
  }

  inline int32_t flush(int timeout_ms) {
    _producer->outq_len();
    return _producer->flush(timeout_ms);
  }

  private:
  class MyHashPartitionerCb : public RdKafka::PartitionerCb
  {
    public:
    int32_t partitioner_cb(const RdKafka::Topic *topic, const std::string *key, int32_t partition_cnt, void *msg_opaque);
  };

  // better to have a static config of nr of parititions
  class MyDeliveryReportCb : public RdKafka::DeliveryReportCb
  {
    public:
    MyDeliveryReportCb();
    virtual void dr_cb(RdKafka::Message &message);
    inline RdKafka::ErrorCode status() const {
      return _status;
    }
    private:
    RdKafka::ErrorCode _status;
  };

  const std::string                  _topic;
  std::unique_ptr<RdKafka::Topic>    _rd_topic;
  std::unique_ptr<RdKafka::Producer> _producer;
  bool                               _closed;
  size_t                             _nr_of_partitions;
  uint64_t                           _msg_cnt;
  uint64_t                           _msg_bytes;
  MyHashPartitionerCb                _default_partitioner;
  MyDeliveryReportCb                 _delivery_report_cb;
};
} // namespace


