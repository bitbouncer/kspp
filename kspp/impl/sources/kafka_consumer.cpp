#include <boost/log/trivial.hpp>
#include "kafka_consumer.h"

#define LOGPREFIX_ERROR BOOST_LOG_TRIVIAL(error) << BOOST_CURRENT_FUNCTION << ", topic:" << _topic << ":" << _partition
#define LOG_INFO(EVENT)  BOOST_LOG_TRIVIAL(info) << "kafka_consumer: " << EVENT << ", topic:" << _topic << ":" << _partition

namespace kspp {
kafka_consumer::kafka_consumer(std::string brokers, std::string topic, int32_t partition, std::string consumer_group)
  : _brokers(brokers)
  , _topic(topic)
  , _partition(partition)
  , _consumer_group(consumer_group)
  , _eof(false)
  , _msg_cnt(0)
  , _msg_bytes(0)
  , _closed(false) {
  _topic_partition.push_back(RdKafka::TopicPartition::create(_topic, _partition));
  if (!init()) {
    exit(-1);
  }
 }

kafka_consumer::~kafka_consumer() {
  if (!_closed)
    close();
  close();
  
  for (auto i : _topic_partition) // should be exactly 1
    delete i;
}

void kafka_consumer::close() {
  if (_closed)
    return;
  _closed = true;
  if (_consumer) {
    _consumer->close();
    LOG_INFO("close") << ", consumed " << _msg_cnt << " messages (" << _msg_bytes << " bytes)";
  }
  //_rd_topic = nullptr;
  _consumer = nullptr;
}

bool kafka_consumer::init() {
  /*
  * Create configuration objects
  */
  std::unique_ptr<RdKafka::Conf> conf(RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL));
  /*
  * Set configuration properties
  */
  std::string errstr;

  if (conf->set("metadata.broker.list", _brokers, errstr) != RdKafka::Conf::CONF_OK) {
    LOGPREFIX_ERROR << ", failed to set metadata.broker.list " << errstr;
    return false;
  }

  if (conf->set("api.version.request", "true", errstr) != RdKafka::Conf::CONF_OK) {
    LOGPREFIX_ERROR << ", failed to set api.version.request " << errstr;
    return false;
  }

  if (conf->set("socket.nagle.disable", "true", errstr) != RdKafka::Conf::CONF_OK) {
    LOGPREFIX_ERROR << ", failed to set socket.nagle.disable " << errstr;
    return false;
  }

  if (conf->set("enable.auto.commit", "false", errstr) != RdKafka::Conf::CONF_OK) {
  LOGPREFIX_ERROR << ", failed to set auto.commit.enable " << errstr;
  return false;
  }

  if (conf->set("auto.commit.interval.ms", "5000", errstr) != RdKafka::Conf::CONF_OK) {
    LOGPREFIX_ERROR << ", failed to set auto.commit.interval.ms " << errstr;
    return false;
  }

  if (conf->set("enable.auto.offset.store", "false", errstr) != RdKafka::Conf::CONF_OK) {
    LOGPREFIX_ERROR << ", failed to set enable.auto.offset.store " << errstr;
    return false;
  }

  if (conf->set("auto.offset.reset", "earliest", errstr) != RdKafka::Conf::CONF_OK) {
    LOGPREFIX_ERROR << ", failed to set auto.offset.reset " << errstr;
    return false;
  }

  if (_consumer_group.size()) {
    if (conf->set("group.id", _consumer_group, errstr) != RdKafka::Conf::CONF_OK) {
      LOGPREFIX_ERROR << ", failed to set group " << errstr;
      return false;
    }
  }

  if (conf->set("enable.partition.eof", "true", errstr) != RdKafka::Conf::CONF_OK) {
      LOGPREFIX_ERROR << ", failed to set enable.partition.eof " << errstr;
      return false;
  }

  /*
  * Create consumer using accumulated global configuration.
  */
  _consumer = std::unique_ptr<RdKafka::KafkaConsumer>(RdKafka::KafkaConsumer::create(conf.get(), errstr));
  if (!_consumer) {
    LOGPREFIX_ERROR << ", failed to create consumer, reason: " << errstr;
    return false;
  }
  LOG_INFO("created");

  ///**
  //* create topic configuration
  //*/
  //std::unique_ptr<RdKafka::Conf> tconf(RdKafka::Conf::create(RdKafka::Conf::CONF_TOPIC));
  ////conf->set("default_topic_conf", tconf.get(), errstr);

  //// to we need both of the following (is this broker of topic config??)
  //if (tconf->set("auto.commit.enable", "false", errstr) != RdKafka::Conf::CONF_OK) {
  //  LOGPREFIX_ERROR << ", failed to set auto.commit.enable " << errstr;
  //  return false;
  //}

  //if (tconf->set("enable.auto.commit", "false", errstr) != RdKafka::Conf::CONF_OK) {
  //  LOGPREFIX_ERROR << ", failed to set enable.auto.commit " << errstr;
  //  return false;
  //}

  //if (tconf->set("auto.offset.reset", "earliest", errstr) != RdKafka::Conf::CONF_OK) {
  //  LOGPREFIX_ERROR << ", failed to set auto.offset.reset " << errstr;
  //  return false;
  //}

  //_rd_topic = std::unique_ptr<RdKafka::Topic>(RdKafka::Topic::create(_consumer.get(), _topic, tconf.get(), errstr));

  //if (!_rd_topic) {
  //  LOGPREFIX_ERROR << ", failed to create topic, reason: " << errstr;
  //  return false;
  //}
  return true;
}

void kafka_consumer::start(int64_t offset) {
  /*
  * Subscribe to topics
  */
  _topic_partition[0]->set_offset(offset);
  RdKafka::ErrorCode err0 = _consumer->assign(_topic_partition);
  if (err0) {
    LOGPREFIX_ERROR << ", failed to subscribe, reason:" << RdKafka::err2str(err0);
    exit(1);
  }

  int64_t low = 0;
  int64_t high = 0;
  RdKafka::ErrorCode err1 = _consumer->query_watermark_offsets(_topic, _partition, &low, &high, 1000);
  if (err1) {
    LOGPREFIX_ERROR << ", failed to query_watermark_offsets, reason:" << RdKafka::err2str(err1);
  }
  if (low == high)
    _eof = true;
}

void kafka_consumer::start() {
  if (_consumer_group.size()) {
    start(RdKafka::Topic::OFFSET_STORED);
  } else {
    LOGPREFIX_ERROR << ", consumer group not defined -> starting from beginning instead of stpred offset";
    start(RdKafka::Topic::OFFSET_BEGINNING);
  }
}

void kafka_consumer::stop() {
  RdKafka::ErrorCode err = _consumer->unassign();
  if (err) {
    LOGPREFIX_ERROR << ", failed to stop, reason:" << RdKafka::err2str(err);
    exit(1);
  }
}

std::unique_ptr<RdKafka::Message> kafka_consumer::consume() {
  std::unique_ptr<RdKafka::Message> msg(_consumer->consume(0));

  switch (msg->err()) {
    case RdKafka::ERR_NO_ERROR:
      _eof = false;
      _msg_cnt++;
      _msg_bytes += msg->len();
      return msg;

    case RdKafka::ERR__TIMED_OUT:
      break;

    case RdKafka::ERR__PARTITION_EOF:
      _eof = true;
      break;

    case RdKafka::ERR__UNKNOWN_TOPIC:
    case RdKafka::ERR__UNKNOWN_PARTITION:
      _eof = true;
      LOGPREFIX_ERROR << ", consume failed: " << msg->errstr();
      break;

    default:
      /* Errors */
      _eof = true;
      LOGPREFIX_ERROR << ", consume failed: " << msg->errstr();
  }
  return nullptr;
}

int32_t kafka_consumer::commit(int64_t offset, bool flush) {
  _topic_partition[0]->set_offset(offset);
  if (flush)
    _consumer->commitSync(_topic_partition);
  else
    _consumer->commitAsync(_topic_partition);
  return 0;
}
}; // namespace