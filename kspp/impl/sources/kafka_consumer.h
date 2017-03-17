#include <memory>
#include <librdkafka/rdkafkacpp.h>
#pragma once

namespace kspp {
  class kafka_consumer
  {
  public:
    kafka_consumer(std::string brokers, std::string topic, size_t partition);

    ~kafka_consumer();

    void close();

    std::unique_ptr<RdKafka::Message> consume();

    inline bool eof() const {
      return _eof;
    }

    inline std::string topic() const {
      return _topic;
    }

    inline uint32_t partition() const {
      return (uint32_t) _partition;
    }

    void start(int64_t offset);

    void commit(int64_t offset, bool flush = false);

  private:
    const std::string                  _topic;
    const size_t                       _partition;
    std::unique_ptr<RdKafka::Topic>    _rd_topic;
    std::unique_ptr<RdKafka::Consumer> _consumer;
    uint64_t                           _msg_cnt;
    uint64_t                           _msg_bytes;
    bool                               _eof;
    bool                               _closed;
  };
};

