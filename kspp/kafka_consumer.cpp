#include <boost/log/trivial.hpp>
#include "kafka_consumer.h"

#define LOGPREFIX_ERROR BOOST_LOG_TRIVIAL(error) << BOOST_CURRENT_FUNCTION << ", topic:" << _topic << ": " << _partition
#define LOGPREFIX_INFO  BOOST_LOG_TRIVIAL(info) << BOOST_CURRENT_FUNCTION << ", topic:" << _topic << ": " << _partition

namespace csi {
kafka_consumer::kafka_consumer(std::string brokers, std::string topic, size_t partition) :
  _topic(topic),
  _partition(partition),
  _eof(false),
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
  _consumer = std::unique_ptr<RdKafka::Consumer>(RdKafka::Consumer::create(conf.get(), errstr));
  if (!_consumer) {
    LOGPREFIX_ERROR << ", failed to create consumer, reason: " << errstr;
    exit(1);
  }
  LOGPREFIX_INFO << ", created consumer " << _consumer->name();

  std::unique_ptr<RdKafka::Conf> tconf2(RdKafka::Conf::create(RdKafka::Conf::CONF_TOPIC));
  _rd_topic = std::unique_ptr<RdKafka::Topic>(RdKafka::Topic::create(_consumer.get(), _topic, tconf2.get(), errstr));

  if (!_rd_topic) {
    LOGPREFIX_ERROR << ", failed to create topic, reason: " << errstr;
    exit(1);
  }
}

kafka_consumer::~kafka_consumer() {
  close();
}

void kafka_consumer::close() {
  if (_consumer) {
    _consumer->stop(_rd_topic.get(), 0);
    LOGPREFIX_INFO << ", consumed " << _msg_cnt << " messages (" << _msg_bytes << " bytes)";
  }
  _rd_topic = NULL;
  _consumer = NULL;
}

void kafka_consumer::start(int64_t offset) {
  /*
  * Subscribe to topics
  */
  RdKafka::ErrorCode err = _consumer->start(_rd_topic.get(), (int32_t) _partition, offset);
  if (err) {
    LOGPREFIX_ERROR << ", failed to subscribe, reason:" << RdKafka::err2str(err);
    exit(1);
  }
}

std::unique_ptr<RdKafka::Message> kafka_consumer::consume() {
  std::unique_ptr<RdKafka::Message> msg(_consumer->consume(_rd_topic.get(), (int32_t) _partition, 0));

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
  return NULL;
}
}; // namespace