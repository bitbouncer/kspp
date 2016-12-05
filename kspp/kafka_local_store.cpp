#include "kafka_local_store.h"

#include <iostream>

namespace csi {


struct uuid
{
  char data[16];
};

struct rocksdb_inner_key
{
  uuid id;
  uint32_t ts;
  int64_t index;
};


kafka_local_store::kafka_local_store(std::string storage_path) :
  _db(NULL) {
  // open DB
  rocksdb::Options options;
  options.create_if_missing = true;
  auto s = rocksdb::DB::Open(options, storage_path, &_db);
  std::cerr << s.ToString() << std::endl;
  assert(s.ok());
}

kafka_local_store::~kafka_local_store() {
  close();
}

void kafka_local_store::close() {
  delete _db;
  _db = NULL;
}

void kafka_local_store::put(RdKafka::Message* msg) {
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
    return _ec;
  }

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
  virtual size_t              len() const { return _playload.size(); }

  /** @returns Message key as string (if applicable) */
  virtual const std::string  *key() const { return &_key; }

  /** @returns Message key as void pointer  (if applicable) */
  virtual const void         *key_pointer() const { return _key.data(); }

  /** @returns Message key's binary length (if applicable) */
  virtual size_t              key_len() const { return _key.size(); }

  /** @returns Message or error offset (if applicable) */
  virtual int64_t             offset() const {
    return 0;
  }

  /** @returns Message timestamp (if applicable) */
  virtual RdKafka::MessageTimestamp    timestamp() const {
    return RdKafka::MessageTimestamp();
  }

  /** @returns The \p msg_opaque as provided to RdKafka::Producer::produce() */
  virtual void               *msg_opaque() const { return (void*) _playload.data(); }

  virtual ~rocksdb_message() {};

  std::string _topic_name;
  int32_t     _partition;
  std::string _key;
  std::string _playload;
  RdKafka::ErrorCode _ec;
};

std::unique_ptr<RdKafka::Message> kafka_local_store::get(const void* key, size_t key_size) {
  std::unique_ptr<rocksdb_message> p(new rocksdb_message(key, key_size));
  rocksdb::Status s = _db->Get(rocksdb::ReadOptions(), rocksdb::Slice((char*) key, key_size), &p->_playload);
  if (s.ok()) {
    return std::unique_ptr<RdKafka::Message>(p.release());
  }
  return NULL;
}
};