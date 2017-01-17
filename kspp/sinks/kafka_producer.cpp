#include <boost/log/trivial.hpp>
#include "kafka_producer.h"

#define LOGPREFIX_ERROR BOOST_LOG_TRIVIAL(error) << BOOST_CURRENT_FUNCTION << ", topic:" << _topic
//#define LOGPREFIX_INFO  BOOST_LOG_TRIVIAL(info) << BOOST_CURRENT_FUNCTION << ", topic:" << _topic
#define LOG_INFO(EVENT)  BOOST_LOG_TRIVIAL(info) << "kafka_producer: " << EVENT << ", topic:" << _topic

namespace kspp {
  /*
  static inline unsigned int djb_hash(const char *str, size_t len) {
    unsigned int hash = 5381;
    for (size_t i = 0; i < len; i++)
      hash = ((hash << 5) + hash) + str[i];
    return hash;
  }
  */

  int32_t kafka_producer::MyHashPartitionerCb::partitioner_cb(const RdKafka::Topic *topic, const std::string *key, int32_t partition_cnt, void *msg_opaque) {
    uintptr_t partition_hash = (uintptr_t) msg_opaque;
    return partition_hash % partition_cnt;
  }

kafka_producer::kafka_producer(std::string brokers, std::string topic) 
  : _topic(topic)
  , _msg_cnt(0)
  , _msg_bytes(0)
  , _closed(false) {
  /*
  * Create configuration objects
  */
  std::unique_ptr<RdKafka::Conf> conf(RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL));
  std::unique_ptr<RdKafka::Conf> tconf(RdKafka::Conf::create(RdKafka::Conf::CONF_TOPIC));

  /*
  * Set configuration properties
  */
  std::string errstr;
  conf->set("metadata.broker.list", brokers, errstr);
  conf->set("api.version.request", "true", errstr);

  //ExampleEventCb ex_event_cb;
  //conf->set("event_cb", &ex_event_cb, errstr);
  conf->set("default_topic_conf", tconf.get(), errstr);

  /*
  * Create consumer using accumulated global configuration.
  */
  _producer = std::unique_ptr<RdKafka::Producer>(RdKafka::Producer::create(conf.get(), errstr));
  if (!_producer) {
    LOGPREFIX_ERROR << ", failed to create producer:" << errstr;
    exit(1);
  }

  LOG_INFO("created");

  std::unique_ptr<RdKafka::Conf> tconf2(RdKafka::Conf::create(RdKafka::Conf::CONF_TOPIC));

  if (tconf2->set("partitioner_cb", &_default_partitioner, errstr) !=
    RdKafka::Conf::CONF_OK) {
    LOGPREFIX_ERROR << ", failed to create partitioner: " << errstr;
    exit(1);
  }

  _rd_topic = std::unique_ptr<RdKafka::Topic>(RdKafka::Topic::create(_producer.get(), _topic, tconf2.get(), errstr));

  if (!_rd_topic) {
    LOGPREFIX_ERROR << ", failed to create topic: " << errstr;
    exit(1);
  }
}

kafka_producer::~kafka_producer() {
  if (!_closed)
    close();
}

void kafka_producer::close() {
  if (_closed)
    return;
  _closed = true;
  while (_producer && _producer->outq_len() > 0) {
    LOG_INFO("closing") << ", waiting for " << _producer->outq_len() << " messages to be written...";
    _producer->poll(1000);
  }
  _rd_topic = NULL;
  _producer = NULL;
  LOG_INFO("closed") << ", produced " << _msg_cnt << " messages (" << _msg_bytes << " bytes)";
}


int kafka_producer::produce(uint32_t partition_hash, rdkafka_memory_management_mode mode, void* key, size_t keysz, void* value, size_t valuesz) {
  RdKafka::ErrorCode resp = _producer->produce(_rd_topic.get(), -1, (int) mode, value, valuesz, key, keysz, (void*) (uintptr_t) partition_hash);
  if (resp != RdKafka::ERR_NO_ERROR) {
    LOGPREFIX_ERROR << ", Produce failed: " << RdKafka::err2str(resp);
    return (int) resp;
  }
  _msg_cnt++;
  _msg_bytes += valuesz;
  return 0;
}

}; // namespace