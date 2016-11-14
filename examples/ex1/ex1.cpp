
#include <iostream>
#include <string>
#include <cstdlib>
#include <cstdio>
#include <csignal>
#include <cstring>
#include  <librdkafka/rdkafkacpp.h>
#include <rocksdb/db.h>
#include <rocksdb/slice.h>
#include <rocksdb/options.h>

static rocksdb::DB* db = NULL;
static bool run = true;
static bool exit_eof = false;
static int eof_cnt = 0;
static int partition_cnt = 0;
static int verbosity = 1;
static long msg_cnt = 0;
static int64_t msg_bytes = 0;
static void sigterm(int sig) {
  run = false;
}

class ExampleEventCb : public RdKafka::EventCb
{
  public:
  void event_cb(RdKafka::Event &event) {

    //print_time();

    switch (event.type()) {
      case RdKafka::Event::EVENT_ERROR:
        std::cerr << "ERROR (" << RdKafka::err2str(event.err()) << "): " <<
          event.str() << std::endl;
        if (event.err() == RdKafka::ERR__ALL_BROKERS_DOWN)
          run = false;
        break;

      case RdKafka::Event::EVENT_STATS:
        std::cerr << "\"STATS\": " << event.str() << std::endl;
        break;

      case RdKafka::Event::EVENT_LOG:
        fprintf(stderr, "LOG-%i-%s: %s\n",
                event.severity(), event.fac().c_str(), event.str().c_str());
        break;

      case RdKafka::Event::EVENT_THROTTLE:
        std::cerr << "THROTTLED: " << event.throttle_time() << "ms by " <<
          event.broker_name() << " id " << (int) event.broker_id() << std::endl;
        break;

      default:
        std::cerr << "EVENT " << event.type() <<
          " (" << RdKafka::err2str(event.err()) << "): " <<
          event.str() << std::endl;
        break;
    }
  }
};


void msg_consume(RdKafka::Message* message, void* opaque) {
  switch (message->err()) {
    case RdKafka::ERR__TIMED_OUT:
      break;

    case RdKafka::ERR_NO_ERROR:
      /* Real message */
      msg_cnt++;
      msg_bytes += message->len();
      if (verbosity >= 3)
        std::cerr << "Read msg at offset " << message->offset() << std::endl;
      RdKafka::MessageTimestamp ts;
      ts = message->timestamp();
      if (verbosity >= 2 &&
          ts.type != RdKafka::MessageTimestamp::MSG_TIMESTAMP_NOT_AVAILABLE) {
        std::string tsname = "?";
        if (ts.type == RdKafka::MessageTimestamp::MSG_TIMESTAMP_CREATE_TIME)
          tsname = "create time";
        else if (ts.type == RdKafka::MessageTimestamp::MSG_TIMESTAMP_LOG_APPEND_TIME)
          tsname = "log append ti me";
        std::cout << "Timestamp: " << tsname << " " << ts.timestamp << std::endl;
      }
      if (verbosity >= 2 && message->key()) {
        //std::cout << "Key: " << *message->key() << std::endl;
      }
      if (verbosity >= 1) {
        //printf("%.*s\n",
        //       static_cast<int>(message->len()),
        //       static_cast<const char *>(message->payload()));
      }

      if (!message->key())
        break;
      if (message->len()) {
        rocksdb::Status s;
        s = db->Put(rocksdb::WriteOptions(), rocksdb::Slice((char*) message->key_pointer(), message->key()->size()), rocksdb::Slice((char*) message->payload(), message->len()));
      } else {
        rocksdb::Status s;
        s = db->Delete(rocksdb::WriteOptions(), rocksdb::Slice((char*) message->key_pointer(), message->key()->size()));
      }
      break;

    case RdKafka::ERR__PARTITION_EOF:
      /* Last message */
      if (exit_eof && ++eof_cnt == partition_cnt) {
        std::cerr << "%% EOF reached for all " << partition_cnt <<
          " partition(s)" << std::endl;
        run = false;
      }
      break;

    case RdKafka::ERR__UNKNOWN_TOPIC:
    case RdKafka::ERR__UNKNOWN_PARTITION:
      std::cerr << "Consume failed: " << message->errstr() << std::endl;
      run = false;
      break;

    default:
      /* Errors */
      std::cerr << "Consume failed: " << message->errstr() << std::endl;
      run = false;
  }
}



int main(int argc, char **argv) {
  std::string brokers = "localhost";
  std::string errstr;
  std::string topic_str;
  std::string mode;
  std::string debug;
  std::vector<std::string> topics;

  exit_eof = true;
  int opt;

  /*
   * Create configuration objects
   */
  RdKafka::Conf *conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
  RdKafka::Conf *tconf = RdKafka::Conf::create(RdKafka::Conf::CONF_TOPIC);

  /*
   * Set configuration properties
   */
  conf->set("metadata.broker.list", brokers, errstr);


  ExampleEventCb ex_event_cb;
  conf->set("event_cb", &ex_event_cb, errstr);

  conf->set("default_topic_conf", tconf, errstr);
  delete tconf;

  signal(SIGINT, sigterm);
  signal(SIGTERM, sigterm);

  std::string topic_name = "vast-playlist-B1";
  std::string kDBPath = "C:\\tmp\\ex1\\" + topic_name + "_0";

  //
  // open DB
  using namespace rocksdb;
  Options options;
  options.create_if_missing = true;
  Status s = DB::Open(options, kDBPath, &db);
  std::cerr << s.ToString() << std::endl;
  assert(s.ok());

  // create column family
  //ColumnFamilyHandle* cf;
  //s = db->CreateColumnFamily(ColumnFamilyOptions(), "new_cf", &cf);
  //assert(s.ok());

  // close DB
  //delete cf;
  //delete db;

  // open DB with two column families
  //std::vector<ColumnFamilyDescriptor> column_families;
  // have to open default column family
  //column_families.push_back(ColumnFamilyDescriptor(
    //kDefaultColumnFamilyName, ColumnFamilyOptions()));
  // open the new one, too
  //column_families.push_back(ColumnFamilyDescriptor(
 //    "new_cf", ColumnFamilyOptions()));
  // std::vector<ColumnFamilyHandle*> handles;
  //s = DB::Open(Options(), kDBPath, &db);
  //assert(s.ok());
  //


  /*
   * Consumer mode
   */

  /*
   * Create consumer using accumulated global configuration.
   */
  RdKafka::Consumer *consumer = RdKafka::Consumer::create(conf, errstr);
  if (!consumer) {
    std::cerr << "Failed to create consumer: " << errstr << std::endl;
    exit(1);
  }

  delete conf;

  std::cout << "% Created consumer " << consumer->name() << std::endl;


  RdKafka::Conf *tconf2 = RdKafka::Conf::create(RdKafka::Conf::CONF_TOPIC);
  RdKafka::Topic *topic = RdKafka::Topic::create(consumer, topic_name, tconf2, errstr);
  delete tconf2;

  if (!topic) {
    std::cerr << "Failed to create topic: " << errstr << std::endl;
    exit(1);
  }

    /*
     * Subscribe to topics
     */
  RdKafka::ErrorCode err = consumer->start(topic, 0, RdKafka::Topic::OFFSET_BEGINNING);
  if (err) {
    std::cerr << "Failed to subscribe to " << topics.size() << " topics: "
      << RdKafka::err2str(err) << std::endl;
    exit(1);
  }

  /*
   * Consume messages
   */
  partition_cnt = 1;

  while (run) {
    RdKafka::Message *msg = consumer->consume(topic, 0, 10);
    msg_consume(msg, NULL);
    delete msg;
  }

  /*
   * Stop consumer
   */
  consumer->stop(topic, 0);
  delete consumer;

  std::cerr << "% Consumed " << msg_cnt << " messages ("
    << msg_bytes << " bytes)" << std::endl;

  
  delete db;

/*
 * Wait for RdKafka to decommission.
 * This is not strictly needed (with check outq_len() above), but
 * allows RdKafka to clean up all its resources before the application
 * exits so that memory profilers such as valgrind wont complain about
 * memory leaks.
 */
  RdKafka::wait_destroyed(5000);

  return 0;
}
