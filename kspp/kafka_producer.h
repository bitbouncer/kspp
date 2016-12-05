#pragma once
#include <memory>
#include <functional>
#include <sstream>
#include <librdkafka/rdkafkacpp.h>
#pragma once

namespace csi {
class kafka_producer
{
  public:
  enum rdkafka_memory_management_mode { NO_COPY=0, FREE=1, COPY=2 };

  kafka_producer(std::string brokers, std::string topic);
  ~kafka_producer();
  
  int produce(int32_t partition, rdkafka_memory_management_mode mode, void* key, size_t keysz, void* value, size_t valuesz);

  void close();
  //int32_t nr_of_partitions() const;
  std::string topic() const { return _topic; }

  size_t queue_len();
  void poll(int timeout);

  private:
  const std::string  _topic;
  RdKafka::Topic*    _rd_topic;
  RdKafka::Producer* _producer;
 
  //int32_t  _nr_of_partitions;
  uint64_t _msg_cnt;
  uint64_t _msg_bytes;
};

template<class K, class V, class codec>
class kafka_producer2
{
public:
  typedef std::function<uint32_t(const K& key, const V& val)> partitioner;

  //enum rdkafka_memory_management_mode { NO_COPY = 0, FREE = 1, COPY = 2 };
  kafka_producer2(std::string brokers, std::string topic, std::shared_ptr<codec> codec, partitioner p) : _impl(brokers, topic), _codec(codec), _partitioner(p) {}
  ~kafka_producer2() {}

  int produce(const K& key, const V& val) {
    void* kp = NULL;
    void* vp = NULL;
    size_t ksize = 0;
    size_t vsize = 0;
    {
      std::stringstream ks;
      ksize = _codec->encode(key, ks);
      kp = malloc(ksize);
      ks.read((char*) kp, ksize);
    }
    {
      std::stringstream vs;
      vsize = _codec->encode(val, vs);
      vp = malloc(vsize);
      vs.read((char*) vp, vsize);
    }
    return _impl.produce(_partitioner(key, val), kafka_producer::FREE, kp, ksize, vp, vsize);
  }

  inline void close() { return _impl.close();  }
  inline size_t queue_len() { return _impl.queue_len(); }
  inline std::string topic() const { return _impl.topic(); }
  inline void poll(int timeout) { return _impl.poll(timeout); }
  inline int32_t nr_of_partitions() const { return _impl.nr_of_partitions(); }
private:
  kafka_producer         _impl;
  std::shared_ptr<codec> _codec;
  partitioner            _partitioner;
};


}; // namespace


