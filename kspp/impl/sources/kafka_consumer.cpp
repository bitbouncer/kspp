#include <thread>
#include <chrono>
#include <boost/log/trivial.hpp>
#include "kafka_consumer.h"
#include <kspp/impl/kafka_utils.h>

#define LOGPREFIX_ERROR BOOST_LOG_TRIVIAL(error) << BOOST_CURRENT_FUNCTION << ", topic:" << _topic << ":" << _partition
#define LOG_INFO(EVENT)  BOOST_LOG_TRIVIAL(info) << "kafka_consumer: " << EVENT << ", topic:" << _topic << ":" << _partition

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

  if (conf->set("auto.commit.interval.ms", "5000", errstr) != RdKafka::Conf::CONF_OK) {
    LOGPREFIX_ERROR << ", failed to set auto.commit.interval.ms " << errstr;
    exit(-1);
  }

  if (conf->set("enable.auto.offset.store", "false", errstr) != RdKafka::Conf::CONF_OK) {
    LOGPREFIX_ERROR << ", failed to set enable.auto.offset.store " << errstr;
    exit(-1);
  }

  if (conf->set("auto.offset.reset", "earliest", errstr) != RdKafka::Conf::CONF_OK) {
    LOGPREFIX_ERROR << ", failed to set auto.offset.reset " << errstr;
    exit(-1);
  }

  if (conf->set("group.id", _consumer_group, errstr) != RdKafka::Conf::CONF_OK) {
    LOGPREFIX_ERROR << ", failed to set group " << errstr;
    exit(-1);
  }

  if (conf->set("enable.partition.eof", "true", errstr) != RdKafka::Conf::CONF_OK) {
    LOGPREFIX_ERROR << ", failed to set enable.partition.eof " << errstr;
    exit(-1);
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
  kspp::kafka::wait_for_group(brokers, consumer_group);
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
  
  //_topic_partition[0]->set_offset(0);
  //_consumer->position(_topic_partition);

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
  start(RdKafka::Topic::OFFSET_STORED);
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
  if (offset < 0 ) {
    int64_t low = 0;
    int64_t high = 0;
    RdKafka::ErrorCode ec = _consumer->query_watermark_offsets(_topic, _partition, &low, &high, 1000);
    if (ec) {
      LOGPREFIX_ERROR << ", failed to query_watermark_offsets, reason:" << RdKafka::err2str(ec);
      return ec;
    }
    auto sz = high - low;
    if ((high - low) == 0) { // empty dataset - nothing to commit
      LOG_INFO("commit") << ", nothing to commit, low: " << low << ", high: " << high;
      return 0;
    }

    if (offset==-1)
      offset = high - 1;
    else
      offset = low - 1;
  } else {   // debug only...
    int64_t low = 0;
    int64_t high = 0;
    RdKafka::ErrorCode ec = _consumer->query_watermark_offsets(_topic, _partition, &low, &high, 1000);
    if (ec) {
      LOGPREFIX_ERROR << ", failed to query_watermark_offsets, reason:" << RdKafka::err2str(ec);
      return ec;
    }
    LOG_INFO("commit") << ", " << offset << " highwatermark=" << high << " behind " << (high - 1) - offset << " messages";
  }

  _can_be_committed = offset;
  RdKafka::ErrorCode ec = RdKafka::ERR_NO_ERROR;
  if (flush) {
    _topic_partition[0]->set_offset(_can_be_committed);
    ec = _consumer->commitSync(_topic_partition);
    if (ec != RdKafka::ERR_NO_ERROR) {
      LOGPREFIX_ERROR << ", failed to commit, reason:" << RdKafka::err2str(ec);
    }
  } else if ((_last_committed + _max_pending_commits) < _can_be_committed) {
    _topic_partition[0]->set_offset(_can_be_committed);
    ec = _consumer->commitAsync(_topic_partition);
    if (ec != RdKafka::ERR_NO_ERROR) {
      LOGPREFIX_ERROR << ", failed to commit, reason:" << RdKafka::err2str(ec);
    }
  } // add timeout based 
  if (ec == RdKafka::ERR_NO_ERROR)
    _last_committed = _can_be_committed;
  return ec;
}
}; // namespace