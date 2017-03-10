#include <boost/log/trivial.hpp>
#include "kafka_producer.h"

#define LOGPREFIX_ERROR BOOST_LOG_TRIVIAL(error) << BOOST_CURRENT_FUNCTION << ", topic:" << _topic
#define LOG_INFO(EVENT)  BOOST_LOG_TRIVIAL(info) << "kafka_producer: " << EVENT << ", topic:" << _topic

namespace kspp {
  int32_t kafka_producer::MyHashPartitionerCb::partitioner_cb(const RdKafka::Topic *topic, const std::string *key, int32_t partition_cnt, void *msg_opaque) {
    uintptr_t partition_hash = (uintptr_t) msg_opaque;
    int32_t partition = partition_hash % partition_cnt;
    return partition;
  }

  kafka_producer::MyDeliveryReportCb::MyDeliveryReportCb() :
    _status(RdKafka::ErrorCode::ERR_NO_ERROR) {
  }

  void kafka_producer::MyDeliveryReportCb::dr_cb(RdKafka::Message& message) {
    if (message.err() == RdKafka::ErrorCode::ERR_NO_ERROR) {
      if (message.offset() > 0)
        _cursor[message.partition()] = message.offset(); // this is not good ... it seems stat the last message of a batch has the server offset stamped into it... we only want OUR message number that came upstream
    } else {
      _status = message.err();
    }
  };

  int64_t kafka_producer::MyDeliveryReportCb::cursor() const {
    int64_t m = INT64_MAX;
    for (auto& i : _cursor)
      m = std::min<int64_t>(m, i.second);
    return m;
  }

kafka_producer::kafka_producer(std::string brokers, std::string topic) 
  : _topic(topic)
  , _msg_cnt(0)
  , _msg_bytes(0)
  , _closed(false) {
  std::string errstr;
  /*
  * Create configuration objects
  */
  std::unique_ptr<RdKafka::Conf> conf(RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL));

  if (conf->set("dr_cb", &_delivery_report_cb, errstr) != RdKafka::Conf::CONF_OK) {
    LOGPREFIX_ERROR << ", failed to add _delivery_report_cb " << errstr;
    exit(1);
  }

  /*
  * Set configuration properties
  */
  if (conf->set("metadata.broker.list", brokers, errstr) != RdKafka::Conf::CONF_OK) {
    LOGPREFIX_ERROR << ", failed to set metadata.broker.list " << errstr;
    exit(1);
  }

  if (conf->set("api.version.request", "true", errstr) != RdKafka::Conf::CONF_OK) {
    LOGPREFIX_ERROR << ", failed to set api.version.request " << errstr;
    exit(1);
  }

  if (conf->set("queue.buffering.max.ms", "100", errstr) != RdKafka::Conf::CONF_OK) {
    LOGPREFIX_ERROR << ", failed to set queue.buffering.max.ms " << errstr;
    exit(1);
  }

  if (conf->set("socket.blocking.max.ms", "100", errstr) != RdKafka::Conf::CONF_OK) {
    LOGPREFIX_ERROR << ", failed to set socket.blocking.max.ms " << errstr;
    exit(1);
  }

  if (conf->set("socket.nagle.disable", "true", errstr) != RdKafka::Conf::CONF_OK) {
    LOGPREFIX_ERROR << ", failed to set socket.nagle.disable=true " << errstr;
    exit(1);
  }

  //ExampleEventCb ex_event_cb;
  //conf->set("event_cb", &ex_event_cb, errstr);

  /*
  * Create producer using accumulated global configuration.
  */
  _producer = std::unique_ptr<RdKafka::Producer>(RdKafka::Producer::create(conf.get(), errstr));
  if (!_producer) {
    LOGPREFIX_ERROR << ", failed to create producer:" << errstr;
    exit(1);
  }

  std::unique_ptr<RdKafka::Conf> tconf(RdKafka::Conf::create(RdKafka::Conf::CONF_TOPIC));

  if (conf->set("default_topic_conf", tconf.get(), errstr) != RdKafka::Conf::CONF_OK) {
    LOGPREFIX_ERROR << ", failed to set default_topic_conf " << errstr;
    exit(1);
  }

  if (tconf->set("partitioner_cb", &_default_partitioner, errstr) != RdKafka::Conf::CONF_OK) {
    LOGPREFIX_ERROR << ", failed to create partitioner: " << errstr;
    exit(1);
  }

  _rd_topic = std::unique_ptr<RdKafka::Topic>(RdKafka::Topic::create(_producer.get(), _topic, tconf.get(), errstr));

  if (!_rd_topic) {
    LOGPREFIX_ERROR << ", failed to create topic: " << errstr;
    exit(1);
  }

  LOG_INFO("created");
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
  _rd_topic = nullptr;
  _producer = nullptr;
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