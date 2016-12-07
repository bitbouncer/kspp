#include "kafka_consumer.h"

#include <iostream>

namespace csi {
kafka_consumer::kafka_consumer(std::string brokers, std::string topic, int32_t partition) :
  _topic(topic),
  _consumer(NULL),
  _rd_topic(NULL),
  _partition(partition),
  _eof(false),
  _msg_cnt(0),
  _msg_bytes(0) {
  /*
  * Create configuration objects
  */
  RdKafka::Conf *conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
  RdKafka::Conf *tconf = RdKafka::Conf::create(RdKafka::Conf::CONF_TOPIC);

  /*
  * Set configuration properties
  */
  std::string errstr;
  conf->set("metadata.broker.list", brokers, errstr);

  //ExampleEventCb ex_event_cb;
  //conf->set("event_cb", &ex_event_cb, errstr);

  conf->set("default_topic_conf", tconf, errstr);
  delete tconf;

  /*
  * Create consumer using accumulated global configuration.
  */
  _consumer = RdKafka::Consumer::create(conf, errstr);
  if (!_consumer) {
    std::cerr << "Failed to create consumer: " << errstr << std::endl;
    exit(1);
  }

  delete conf;

  std::cout << "% Created consumer " << _consumer->name() << std::endl;


  RdKafka::Conf *tconf2 = RdKafka::Conf::create(RdKafka::Conf::CONF_TOPIC);
  _rd_topic = RdKafka::Topic::create(_consumer, _topic, tconf2, errstr);
  delete tconf2;

  if (!_rd_topic) {
    std::cerr << "Failed to create topic: " << errstr << std::endl;
    exit(1);
  }
}

kafka_consumer::~kafka_consumer() {
  close();
}

void kafka_consumer::close() {
  if (_consumer) {
    _consumer->stop(_rd_topic, 0);
    std::cerr << _topic << ":" << _partition << ", Consumed " << _msg_cnt << " messages (" << _msg_bytes << " bytes)" << std::endl;
  }
  delete _rd_topic;
  delete _consumer;
  _rd_topic = NULL;
  _consumer = NULL;
}

void kafka_consumer::start(int64_t offset) {
  /*
  * Subscribe to topics
  */
  RdKafka::ErrorCode err = _consumer->start(_rd_topic, _partition, offset);
  if (err) {
    std::cerr << "Failed to subscribe to " << _topic << ", " << RdKafka::err2str(err) << std::endl;
    exit(1);
  }
}

std::unique_ptr<RdKafka::Message> kafka_consumer::consume() {
  std::unique_ptr<RdKafka::Message> msg(_consumer->consume(_rd_topic, _partition, 0));

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
      std::cerr << "Consume failed: " << msg->errstr() << std::endl;
      //run = false;
      break;

    default:
      /* Errors */
      _eof = true;
      std::cerr << "Consume failed: " << msg->errstr() << std::endl;
  }
  return NULL;
}
}; // namespace