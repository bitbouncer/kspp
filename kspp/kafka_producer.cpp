#include <boost/log/trivial.hpp>
#include "kafka_producer.h"

#define LOGPREFIX_ERROR BOOST_LOG_TRIVIAL(error) << BOOST_CURRENT_FUNCTION << ", topic:" << _topic
#define LOGPREFIX_INFO  BOOST_LOG_TRIVIAL(info) << BOOST_CURRENT_FUNCTION << ", topic:" << _topic

namespace csi {
kafka_producer::kafka_producer(std::string brokers, std::string topic) :
  _topic(topic),
  _msg_cnt(0),
  _msg_bytes(0) {
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

  LOGPREFIX_INFO << ", created producer " << _producer->name();

  std::unique_ptr<RdKafka::Conf> tconf2(RdKafka::Conf::create(RdKafka::Conf::CONF_TOPIC));
  _rd_topic = std::unique_ptr<RdKafka::Topic>(RdKafka::Topic::create(_producer.get(), _topic, tconf2.get(), errstr));

  if (!_rd_topic) {
    LOGPREFIX_ERROR << ", failed to create topic: " << errstr;
    exit(1);
  }
}

kafka_producer::~kafka_producer() {
  close();
}

int kafka_producer::produce(int32_t partition, rdkafka_memory_management_mode mode, void* key, size_t keysz, void* value, size_t valuesz) {
  RdKafka::ErrorCode resp = _producer->produce(_rd_topic.get(), partition, (int) mode, value, valuesz, key, keysz, NULL);
  if (resp != RdKafka::ERR_NO_ERROR) {
    LOGPREFIX_ERROR << ", Produce failed: " << RdKafka::err2str(resp);
    return (int) resp;
  }
  _msg_cnt++;
  _msg_bytes += valuesz;
  return 0;
}

void kafka_producer::close() {
  while (_producer && _producer->outq_len() > 0) {
    LOGPREFIX_INFO << "Waiting for " << _producer->outq_len();
    _producer->poll(1000);
  }
  _rd_topic = NULL;
  _producer = NULL;
  LOGPREFIX_INFO << ", produced " << _msg_cnt << " messages (" << _msg_bytes << " bytes)";
}
}; // namespace