#include "kspp.h"

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

  /*
  * Subscribe to topics
  */
  RdKafka::ErrorCode err = _consumer->start(_rd_topic, 0, RdKafka::Topic::OFFSET_BEGINNING);
  if (err) {
    std::cerr << "Failed to subscribe to " << _topic << ", " << RdKafka::err2str(err) << std::endl;
    exit(1);
  }
}


kafka_consumer::~kafka_consumer() {
  /*
  * Stop consumer
  */
  _consumer->stop(_rd_topic, 0);
  delete _consumer;

  delete _rd_topic;

  std::cerr << "% Consumed " << _msg_cnt << " messages ("
    << _msg_bytes << " bytes)" << std::endl;
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

rockdb_impl::rockdb_impl(std::string storage_path) :
  _db(NULL) {
  // open DB
  rocksdb::Options options;
  options.create_if_missing = true;
  auto s = rocksdb::DB::Open(options, storage_path, &_db);
  std::cerr << s.ToString() << std::endl;
  assert(s.ok());
}

rockdb_impl::~rockdb_impl() {
  delete _db;
}

void rockdb_impl::put(RdKafka::Message* msg) {
  //auto ts = msg->timestamp();
  if (!msg->key())
    return;
  if (msg->len()) {
    rocksdb::Status s;
    s = _db->Put(rocksdb::WriteOptions(), rocksdb::Slice((char*) msg->key_pointer(), msg->key()->size()), rocksdb::Slice((char*) msg->payload(), msg->len()));
  } else {
    rocksdb::Status s;
    s = _db->Delete(rocksdb::WriteOptions(), rocksdb::Slice((char*) msg->key_pointer(), msg->key()->size()));
  }
}

//if (verbosity >= 3)
//  std::cerr << "Read msg at offset " << message->offset() << std::endl;
/*
if (verbosity >= 2 &&
ts.type != RdKafka::MessageTimestamp::MSG_TIMESTAMP_NOT_AVAILABLE) {
std::string tsname = "?";
if (ts.type == RdKafka::MessageTimestamp::MSG_TIMESTAMP_CREATE_TIME)
tsname = "create time";
else if (ts.type == RdKafka::MessageTimestamp::MSG_TIMESTAMP_LOG_APPEND_TIME)
tsname = "log append ti me";
std::cout << "Timestamp: " << tsname << " " << ts.timestamp << std::endl;
}
*/
//if (verbosity >= 2 && message->key()) {
//std::cout << "Key: " << *message->key() << std::endl;
//}
//if (verbosity >= 1) {
//printf("%.*s\n",
//       static_cast<int>(message->len()),
//       static_cast<const char *>(message->payload()));
//}

class rocksdb_message : public RdKafka::Message
{
  public:
  rocksdb_message(const void* key, size_t key_size) : _key((const char*) key, key_size) {}

  /**
  * @brief Accessor functions*
  * @remark Not all fields are present in all types of callbacks.
  */

  /** @returns The error string if object represent an error event,
  *           else an empty string. */
  virtual std::string         errstr() const {
    return "";
  }

  /** @returns The error code if object represents an error event, else 0. */
  virtual RdKafka::ErrorCode           err() const {
    return _ec;  }

  /** @returns the RdKafka::Topic object for a message (if applicable),
  *            or NULL if a corresponding RdKafka::Topic object has not been
  *            explicitly created with RdKafka::Topic::create().
  *            In this case use topic_name() instead. */
  virtual RdKafka::Topic              *topic() const { return NULL; }

  /** @returns Topic name (if applicable, else empty string) */
  virtual std::string         topic_name() const { return _topic_name; }

  /** @returns Partition (if applicable) */
  virtual int32_t             partition() const { return _partition; }

  /** @returns Message payload (if applicable) */
  virtual void               *payload() const { return (void*) _playload.data(); }

  /** @returns Message payload length (if applicable) */
  virtual size_t              len() const {  return _playload.size();  }

  /** @returns Message key as string (if applicable) */
virtual const std::string  *key() const { return &_key; }

  /** @returns Message key as void pointer  (if applicable) */
virtual const void         *key_pointer() const { return _key.data(); }

  /** @returns Message key's binary length (if applicable) */
virtual size_t              key_len() const { return _key.size();  }

  /** @returns Message or error offset (if applicable) */
virtual int64_t             offset() const {
  return 0;
}

  /** @returns Message timestamp (if applicable) */
virtual RdKafka::MessageTimestamp    timestamp() const {
  return RdKafka::MessageTimestamp();
}

  /** @returns The \p msg_opaque as provided to RdKafka::Producer::produce() */
virtual void               *msg_opaque() const {return (void*) _playload.data(); }

virtual ~rocksdb_message() {};

  std::string _topic_name;
  int32_t     _partition;
  std::string _key;
  std::string _playload;
  RdKafka::ErrorCode _ec;
};



std::unique_ptr<RdKafka::Message> rockdb_impl::get(const void* key, size_t key_size) {
  //std::string value;
  std::unique_ptr<rocksdb_message> p(new rocksdb_message(key, key_size));
  rocksdb::Status s = _db->Get(rocksdb::ReadOptions(), rocksdb::Slice((char*) key, key_size), &p->_playload);
  if (s.ok()) {
    return std::unique_ptr<RdKafka::Message>(p.release());
  }
  //if (s.ok()) s = db->Put(rocksdb::WriteOptions(), key2, value);
  return NULL;
}
};