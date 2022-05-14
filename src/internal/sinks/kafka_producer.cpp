#include <kspp/internal/sinks/kafka_producer.h>
#include <thread>
#include <glog/logging.h>
#include <kspp/topology_builder.h>
#include <kspp/internal/rd_kafka_utils.h>
#include <kspp/cluster_metadata.h>

using namespace std::chrono_literals;

namespace kspp {
  struct producer_user_data {
    producer_user_data(void *key, size_t keysz, void *val, size_t valsz, uint32_t hash,
                       std::shared_ptr<event_done_marker> marker)
        : partition_hash(hash)
          , done_marker(marker)
          , key_ptr(key)
          , key_sz(keysz)
          , val_ptr(val)
          , val_sz(valsz) {
    }

    ~producer_user_data() {
      if (key_ptr)
        free(key_ptr);
      if (val_ptr)
        free(val_ptr);

      key_ptr = nullptr;
      val_ptr = nullptr;
    }

    uint32_t partition_hash;
    std::shared_ptr<event_done_marker> done_marker;
    void *key_ptr;
    size_t key_sz;
    void *val_ptr;
    size_t val_sz;
  };

  int32_t kafka_producer::MyHashPartitionerCb::partitioner_cb(const RdKafka::Topic *topic, const std::string *key,
                                                              int32_t partition_cnt, void *msg_opaque) {
    producer_user_data *extra = (producer_user_data *) msg_opaque;
    int32_t partition = extra->partition_hash % partition_cnt;
    return partition;
  }

  kafka_producer::MyDeliveryReportCb::MyDeliveryReportCb() :
      _status(RdKafka::ErrorCode::ERR_NO_ERROR) {}

  void kafka_producer::MyDeliveryReportCb::dr_cb(RdKafka::Message &message) {
    producer_user_data *extra = (producer_user_data *) message.msg_opaque();
    assert(extra);

    // if error fail this commit
    if (message.err() != RdKafka::ErrorCode::ERR_NO_ERROR) {
      if (extra->done_marker)
        extra->done_marker->fail(message.err());
      _status = message.err();
    }
    delete extra; // kill the marker here...
  }

  void kafka_producer::MyEventCb::event_cb(RdKafka::Event &event) {
    switch (event.type()) {
      case RdKafka::Event::EVENT_ERROR:
        LOG(ERROR) << RdKafka::err2str(event.err()) << " " << event.str();
        //if (event.err() == RdKafka::ERR__ALL_BROKERS_DOWN) TODO
        //  run = false;
        break;

      case RdKafka::Event::EVENT_STATS:
        LOG(INFO) << "STATS: " << event.str();
        break;

      case RdKafka::Event::EVENT_LOG:
        LOG(INFO) << event.fac() << ", " << event.str();
        break;

      default:
        LOG(INFO) << "EVENT " << event.type() << " (" << RdKafka::err2str(event.err()) << "): " << event.str();
        break;
    }
  }

  kafka_producer::kafka_producer(std::shared_ptr<cluster_config> cconfig, std::string topic)
      : topic_(topic) {
    LOG_IF(FATAL,
           cconfig->get_cluster_metadata()->wait_for_topic_leaders(topic, cconfig->get_cluster_state_timeout()) ==
           false)
            << "failed to wait for topic leaders, topic:" << topic;

    /*
    * Create configuration objects
    */
    std::unique_ptr<RdKafka::Conf> conf(RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL));
    std::unique_ptr<RdKafka::Conf> tconf(RdKafka::Conf::create(RdKafka::Conf::CONF_TOPIC));

    /*
    * Set configuration properties
    */
    try {
      set_broker_config(conf.get(), cconfig.get());

      set_config(conf.get(), "dr_cb", &delivery_report_cb_);
      //set_config(conf.get(), "api.version.request", "true");
      set_config(conf.get(), "queue.buffering.max.ms", std::to_string(cconfig->get_producer_buffering_time().count()));
      //set_config(conf.get(), "socket.blocking.max.ms", std::to_string(cconfig->get_producer_buffering_time().count()));
      set_config(conf.get(), "message.timeout.ms", std::to_string(cconfig->get_producer_message_timeout().count()));
      set_config(conf.get(), "socket.nagle.disable", "true");
      set_config(conf.get(), "socket.max.fails", "1000000");
      set_config(conf.get(), "message.send.max.retries", "1000000");
      set_config(conf.get(), "log.connection.close", "false");
      set_config(conf.get(), "event_cb", &event_cb_);
      set_config(tconf.get(), "partitioner_cb", &default_partitioner_);
      set_config(conf.get(), "default_topic_conf", tconf.get());
    }
    catch (std::invalid_argument &e) {
      LOG(FATAL) << "topic:" << topic_ << ", bad config " << e.what();
      exit(1);
    }

    /*
    * Create producer using accumulated global configuration.
    */
    std::string errstr;
    producer_ = std::unique_ptr<RdKafka::Producer>(RdKafka::Producer::create(conf.get(), errstr));
    if (!producer_) {
      LOG(FATAL) << "topic:" << topic_ << ", failed to create producer:" << errstr;
      exit(1);
    }

    // we have to keep this around otherwise the paritioner does not work...
    rd_topic_ = std::unique_ptr<RdKafka::Topic>(RdKafka::Topic::create(producer_.get(), topic_, tconf.get(), errstr));

    if (!rd_topic_) {
      LOG(FATAL) << "topic:" << topic_ << ", failed to create topic: " << errstr;
      exit(1);
    }

    LOG(INFO) << "topic:" << topic_ << ", kafka producer created";
  }

  kafka_producer::~kafka_producer() {
    if (!closed_)
      close();
  }

  void kafka_producer::close() {
    if (closed_)
      return;
    closed_ = true;

    if (producer_ && producer_->outq_len() > 0) {
      LOG(INFO) << "topic:" << topic_ << ", kafka producer closing - waiting for " << producer_->outq_len()
                << " messages to be written...";
    }

    // quick flush then exit anyway
    auto ec = producer_->flush(2000);
    if (ec) {
      LOG(INFO) << "topic:" << topic_ << ", kafka producer flush did not finish in 2 sec " << RdKafka::err2str(ec);
    }

    // should we still keep trying here???
    /*
    while (_producer && _producer->outq_len() > 0) {
      auto ec = _producer->flush(1000);
      LOG_INFO("closing") << ", still waiting for " << _producer->outq_len() << " messages to be written... " << RdKafka::err2str(ec);
    }
    */

    rd_topic_.reset(nullptr);
    producer_.reset(nullptr);
    LOG(INFO) << "topic:" << topic_ << ", kafka producer closed - produced " << msg_cnt_ << " messages (" << msg_bytes_
              << " bytes)";
  }


  int
  kafka_producer::produce(uint32_t partition_hash, memory_management_mode mode, void *key, size_t keysz, void *value,
                          size_t valuesz, int64_t timestamp, std::shared_ptr<event_done_marker> marker) {
    producer_user_data *user_data = nullptr;
    if (mode == kafka_producer::COPY) {
      void *pkey = malloc(keysz);
      memcpy(pkey, key, keysz);
      key = pkey;

      void *pval = malloc(valuesz);
      memcpy(pval, value, valuesz);
      value = pval;
    }
    user_data = new producer_user_data(key, keysz, value, valuesz, partition_hash, marker);

    RdKafka::ErrorCode ec = producer_->produce(topic_, -1, 0, value, valuesz, key, keysz, timestamp,
                                               user_data); // note not using _rd_topic anymore...?
    if (ec == RdKafka::ERR__QUEUE_FULL) {
      DLOG(INFO) << "kafka_producer, topic:" << topic_ << ", queue full - retrying, msg_count (" << msg_cnt_ << ")";
      delete user_data;
      return ec;
    } else if (ec != RdKafka::ERR_NO_ERROR) {
      LOG(ERROR) << "kafka_producer, topic:" << topic_ << ", produce failed: " << RdKafka::err2str(ec);
      // should this be a fatal?
      delete user_data; // how do we signal failure to send data... the consumer should probably not continue...
      return ec;
    }

    msg_cnt_++;
    msg_bytes_ += (valuesz + keysz);
    return 0;
  }
} // namespace