#include <memory>
#include <librdkafka/rdkafkacpp.h>
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
    
    inline std::string topic() const { 
      return _topic; 
    }
    
    inline int32_t partition() const {
      return _partition;
    }

    void start(int64_t offset);

  private:
    const std::string                  _topic;
    const int32_t                      _partition;
    std::unique_ptr<RdKafka::Topic>    _rd_topic;
    std::unique_ptr<RdKafka::Consumer> _consumer;
    uint64_t                           _msg_cnt;
    uint64_t                           _msg_bytes;
    bool                               _eof;
  };
};

