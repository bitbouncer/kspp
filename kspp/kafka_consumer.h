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
  void close();
  std::unique_ptr<RdKafka::Message> consume();
  inline bool eof() const {
    return _eof;
  }
  std::string topic() const { return _topic; }

  void start(int64_t offset);

  private:
  const std::string  _topic;
  const int32_t      _partition;
  RdKafka::Topic*    _rd_topic;
  RdKafka::Consumer* _consumer;

  uint64_t _msg_cnt;
  uint64_t _msg_bytes;
  bool     _eof;
};

/*
template<class K, class V, class codec>
class kafka_consumer
{
public:
  kafka_consumer(std::string brokers, std::string topic, int32_t partition) : _impl(brokers, topic, partition) {
  }

  ~kafka_consumer();
  std::unique_ptr<RdKafka::Message> consume();
  inline bool eof() const {
    return _impl.eof();
  }
private:
  kafka_consumer_impl _impl;
}
*/

};
