#pragma once
#include <memory>
#include  <librdkafka/rdkafkacpp.h>

#pragma once

namespace csi {
class kafka_consumer
{
  public:
  kafka_consumer(std::string brokers, std::string topic, int32_t partition);
  ~kafka_consumer();
  std::unique_ptr<RdKafka::Message> consume();
  inline bool eof() const {
    return _eof;
  }
  private:
  const std::string  _topic;
  const int32_t      _partition;
  RdKafka::Topic*    _rd_topic;
  RdKafka::Consumer* _consumer;

  uint64_t _msg_cnt;
  uint64_t _msg_bytes;
  bool     _eof;
};
};
