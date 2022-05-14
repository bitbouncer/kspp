#include <memory>
#include <string>
#include <cstdint>
#include <map>
#include <librdkafka/rdkafkacpp.h>
#include <kspp/kspp.h>

#pragma once

namespace kspp {
  class kafka_producer {

  public:
    enum memory_management_mode {
      FREE = 1, COPY = 2
    };

    kafka_producer(std::shared_ptr<cluster_config> config, std::string topic);

    ~kafka_producer();

    void close();

    /**
    produce a message to partition -> (partition_hash % partition_cnt)
    */
    int
    produce(uint32_t partition_hash, memory_management_mode mode, void *key, size_t keysz, void *value, size_t valuesz,
            int64_t timestamp, std::shared_ptr<event_done_marker> autocommit_marker);

    inline std::string topic() const {
      return topic_;
    }

    inline size_t queue_size() const {
      return closed_ ? 0 : producer_->outq_len();
    }

    inline void poll(int timeout) {
      producer_->poll(timeout);
    }

    inline bool good() const {
      return (delivery_report_cb_.status() == RdKafka::ErrorCode::ERR_NO_ERROR);
    }

    inline size_t nr_of_partitions() {
      // this one does not seem to be assigned in cb??
      return nr_of_partitions_;
    }

    inline int32_t flush(int timeout_ms) {
      return (queue_size() == 0) ? 0 : producer_->flush(timeout_ms);
    }

  private:
    class MyHashPartitionerCb : public RdKafka::PartitionerCb {
    public:
      int32_t
      partitioner_cb(const RdKafka::Topic *topic, const std::string *key, int32_t partition_cnt, void *msg_opaque);
    };

    // better to have a static config of nr of parititions
    class MyDeliveryReportCb : public RdKafka::DeliveryReportCb {
    public:
      MyDeliveryReportCb();

      virtual void dr_cb(RdKafka::Message &message);

      inline RdKafka::ErrorCode status() const {
        return _status;
      }

    private:
      RdKafka::ErrorCode _status;
    };


    class MyEventCb : public RdKafka::EventCb {
    public:
      void event_cb(RdKafka::Event &event);
    };


    const std::string topic_;
    std::unique_ptr<RdKafka::Topic> rd_topic_;
    std::unique_ptr<RdKafka::Producer> producer_;
    bool closed_ = false;
    size_t nr_of_partitions_ = 0;
    uint64_t msg_cnt_=0;    // TODO move to metrics
    uint64_t msg_bytes_=0;  // TODO move to metrics
    MyHashPartitionerCb default_partitioner_;
    MyDeliveryReportCb delivery_report_cb_;
    MyEventCb event_cb_;
  };
} // namespace


