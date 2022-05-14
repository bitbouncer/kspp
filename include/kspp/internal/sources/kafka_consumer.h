#include <chrono>
#include <memory>
#include <librdkafka/rdkafkacpp.h>

#pragma once

namespace kspp {
  class cluster_config;

  class kafka_consumer {
  public:
    kafka_consumer(std::shared_ptr<cluster_config> config, std::string topic, int32_t partition,
                   std::string consumer_group, bool check_cluster = true);

    ~kafka_consumer();

    void close();

    std::unique_ptr<RdKafka::Message> consume(int librdkafka_timeout = 0);

    inline bool eof() const {
      return eof_;
    }

    inline std::string topic() const {
      return topic_;
    }

    inline int32_t partition() const {
      return partition_;
    }

    void start(int64_t offset);

    void stop();

    int32_t commit(int64_t offset, bool flush = false);

    inline int64_t commited() const {
      return can_be_committed_;
    }

    int update_eof();

    bool consumer_group_exists(std::string consumer_group, std::chrono::seconds timeout) const;

  private:
    class MyEventCb : public RdKafka::EventCb {
    public:
      void event_cb(RdKafka::Event &event);
    };

    std::shared_ptr<cluster_config> config_;
    const std::string topic_;
    const int32_t partition_;
    const std::string consumer_group_;
    std::vector<RdKafka::TopicPartition *> topic_partition_;
    std::unique_ptr<RdKafka::KafkaConsumer> consumer_;
    int64_t can_be_committed_ = -1;
    int64_t last_committed_ = -1;
    size_t max_pending_commits_ = 5000;
    uint64_t msg_cnt_ = 0 ;    // TODO move to metrics
    uint64_t msg_bytes_ = 0;  // TODO move to metrics
    bool eof_ = false;
    bool closed_ = false;
    MyEventCb event_cb_;
  };
}

