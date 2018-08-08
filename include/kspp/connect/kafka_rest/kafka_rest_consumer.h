#include <chrono>
#include <memory>

#pragma once

namespace kspp {
  class cluster_config;
  class kafka_rest_consumer
  {
  public:
    kafka_rest_consumer(std::shared_ptr<cluster_config> config, std::string topic, int32_t partition, std::string consumer_group);
    ~kafka_rest_consumer();

    void close();

    //std::unique_ptr<RdKafka::Message> consume();

    inline bool eof() const {
      return _eof;
    }

    inline std::string topic() const {
      return _topic;
    }

    inline int32_t partition() const {
      return _partition;
    }

    void start(int64_t offset);

    void stop();

    int32_t commit(int64_t offset, bool flush = false);

    inline int64_t commited() const {
      return _can_be_committed;
    }

    int update_eof();

  private:
    /*
     * class MyEventCb : public RdKafka::EventCb {
    public:
      void event_cb (RdKafka::Event &event);
    };
     */

    std::shared_ptr<cluster_config>         _config;
    const std::string                       _topic;
    const int32_t                           _partition;
    const std::string                       _consumer_group;
    //std::vector<RdKafka::TopicPartition*>   _topic_partition;
    //std::unique_ptr<RdKafka::KafkaConsumer> _consumer;
    int64_t                                 _can_be_committed;
    int64_t                                 _last_committed;
    size_t                                  _max_pending_commits;
    uint64_t                                _msg_cnt;
    uint64_t                                _msg_bytes;
    bool                                    _eof;
    bool                                    _closed;
    //MyEventCb                               _event_cb;
  };
}

