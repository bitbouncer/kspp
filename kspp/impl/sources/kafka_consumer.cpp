#include <thread>
#include <chrono>
#include <boost/log/trivial.hpp>
#include "kafka_consumer.h"
#include <kspp/impl/kafka_utils.h>

#define LOGPREFIX_ERROR BOOST_LOG_TRIVIAL(error) << BOOST_CURRENT_FUNCTION << ", topic:" << _topic << ":" << _partition
#define LOG_INFO(EVENT)  BOOST_LOG_TRIVIAL(info) << "kafka_consumer: " << EVENT << ", topic:" << _topic << ":" << _partition
#define LOG_DEBUG(EVENT)  BOOST_LOG_TRIVIAL(debug) << "kafka_consumer: " << EVENT << ", topic:" << _topic << ":" << _partition

using namespace std::chrono_literals;

namespace kspp {
kafka_consumer::kafka_consumer(std::string brokers, std::string topic, int32_t partition, std::string consumer_group)
  : _brokers(brokers)
  , _topic(topic)
  , _partition(partition)
  , _consumer_group(consumer_group)
  , _can_be_committed(-1)
  , _last_committed(-1)
  , _max_pending_commits(5000)
  , _eof(false)
  , _msg_cnt(0)
  , _msg_bytes(0)
  , _closed(false) {
  _topic_partition.push_back(RdKafka::TopicPartition::create(_topic, _partition));
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
    exit(-1);
  }

  if (conf->set("api.version.request", "true", errstr) != RdKafka::Conf::CONF_OK) {
    LOGPREFIX_ERROR << ", failed to set api.version.request " << errstr;
    exit(-1);
  }

  if (conf->set("socket.nagle.disable", "true", errstr) != RdKafka::Conf::CONF_OK) {
    LOGPREFIX_ERROR << ", failed to set socket.nagle.disable " << errstr;
    exit(-1);
  }

  if (conf->set("enable.auto.commit", "false", errstr) != RdKafka::Conf::CONF_OK) {
    LOGPREFIX_ERROR << ", failed to set auto.commit.enable " << errstr;
    exit(-1);
  }

  // probably not needed
  if (conf->set("auto.commit.interval.ms", "5000", errstr) != RdKafka::Conf::CONF_OK) {
    LOGPREFIX_ERROR << ", failed to set auto.commit.interval.ms " << errstr;
    exit(-1);
  }

  if (conf->set("enable.auto.offset.store", "false", errstr) != RdKafka::Conf::CONF_OK) {
    LOGPREFIX_ERROR << ", failed to set enable.auto.offset.store " << errstr;
    exit(-1);
  }

  //// this seems to be a bug with librdkafka v0.9.4 (works with master)
  //if (conf->set("auto.offset.reset", "earliest", errstr) != RdKafka::Conf::CONF_OK) {
  //  LOGPREFIX_ERROR << ", failed to set auto.offset.reset " << errstr;
  //  //exit(-1);
  //}

  if (conf->set("group.id", _consumer_group, errstr) != RdKafka::Conf::CONF_OK) {
    LOGPREFIX_ERROR << ", failed to set group " << errstr;
    exit(-1);
  }

  if (conf->set("enable.partition.eof", "true", errstr) != RdKafka::Conf::CONF_OK) {
    LOGPREFIX_ERROR << ", failed to set enable.partition.eof " << errstr;
    exit(-1);
  }

  // following are topic configs but they will be passed in default_topic_conf to broker config.
  {
    std::unique_ptr<RdKafka::Conf> tconf(RdKafka::Conf::create(RdKafka::Conf::CONF_TOPIC));

    if (tconf->set("auto.offset.reset", "earliest", errstr) != RdKafka::Conf::CONF_OK) {
      LOGPREFIX_ERROR << ", failed to set auto.offset.reset " << errstr;
      //exit(-1);
    }

    if (conf->set("default_topic_conf", tconf.get(), errstr) != RdKafka::Conf::CONF_OK) {
      LOGPREFIX_ERROR << ", failed to set default_topic_conf " << errstr;
      exit(-1);
    }
  }


  /*
  * Create consumer using accumulated global configuration.
  */
  _consumer = std::unique_ptr<RdKafka::KafkaConsumer>(RdKafka::KafkaConsumer::create(conf.get(), errstr));
  if (!_consumer) {
    LOGPREFIX_ERROR << ", failed to create consumer, reason: " << errstr;
    exit(-1);
  }
  LOG_INFO("created");
  // really try to make sure the partition & group exist before we continue
  kspp::kafka::wait_for_partition(_consumer.get(), _topic, _partition);
  //kspp::kafka::wait_for_group(brokers, consumer_group); something seems to wrong in rdkafka master....
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
    LOG_INFO("closed") << ", consumed " << _msg_cnt << " messages (" << _msg_bytes << " bytes)";
  }
  _consumer = nullptr;
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
  update_eof();
}

void kafka_consumer::start() {
  if (kspp::kafka::group_exists(_brokers, _consumer_group)==0)
    start(RdKafka::Topic::OFFSET_STORED);
  else
    start(-2);
}

void kafka_consumer::stop() {
  RdKafka::ErrorCode err = _consumer->unassign();
  if (err) {
    LOGPREFIX_ERROR << ", failed to stop, reason:" << RdKafka::err2str(err);
    exit(1);
  }
}

int kafka_consumer::update_eof(){
  int64_t low = 0;
  int64_t high = 0;
  RdKafka::ErrorCode ec = _consumer->query_watermark_offsets(_topic, _partition, &low, &high, 1000);
  if (ec) {
    LOGPREFIX_ERROR << ", failed to query_watermark_offsets, reason:" << RdKafka::err2str(ec);
    return ec;
  }
  if (low == high) {
    _eof = true;
  } else {
    auto ec = _consumer->position(_topic_partition);
    if (ec) {
      LOGPREFIX_ERROR << ", failed to query position, reason:" << RdKafka::err2str(ec);
      return ec;
    }
    auto cursor = _topic_partition[0]->offset();
    _eof = (cursor + 1 == high);
  }
  return 0;
}

std::unique_ptr<RdKafka::Message> kafka_consumer::consume() {
  std::unique_ptr<RdKafka::Message> msg(_consumer->consume(0));

  switch (msg->err()) {
    case RdKafka::ERR_NO_ERROR:
      _eof = false;
      _msg_cnt++;
      _msg_bytes += msg->len() + msg->key_len();
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
  if (offset < 0) // not valid
    return 0;

  // you should actually write offset + 1, since a new consumer will start at offset.
  offset = offset + 1;

  if (offset == _last_committed) // already done
    return 0;

  _can_be_committed = offset;
  RdKafka::ErrorCode ec = RdKafka::ERR_NO_ERROR;
  if (flush) {
    LOG_INFO("commiting(flush)") << ", " << _can_be_committed;
    _topic_partition[0]->set_offset(_can_be_committed);
    ec = _consumer->commitSync(_topic_partition);
    if (ec == RdKafka::ERR_NO_ERROR) {
      _last_committed = _can_be_committed;
    } else {
      LOGPREFIX_ERROR << ", failed to commit, reason:" << RdKafka::err2str(ec);
    }
  } else if ((_last_committed + _max_pending_commits) < _can_be_committed) {
    LOG_DEBUG("commiting(lazy)") << ", " << _can_be_committed;
    _topic_partition[0]->set_offset(_can_be_committed);
    ec = _consumer->commitAsync(_topic_partition);
    if (ec == RdKafka::ERR_NO_ERROR) {
      _last_committed = _can_be_committed;
    } else {
      LOGPREFIX_ERROR << ", failed to commit, reason:" << RdKafka::err2str(ec);
    }
  }
  // TBD add time based autocommit 
  return ec;
}
}; // namespace