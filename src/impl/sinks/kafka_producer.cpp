#include <kspp/impl/sinks/kafka_producer.h>
#include <thread>
#include <glog/logging.h>
#include <kspp/topology_builder.h>
#include <kspp/impl/rd_kafka_utils.h>

using namespace std::chrono_literals;

namespace kspp {
  struct producer_user_data
  {
    producer_user_data(void* key, size_t keysz, void* val, size_t valsz, uint32_t hash, std::shared_ptr<commit_chain::autocommit_marker> marker)
        : key_ptr(key)
        , key_sz(keysz)
        , val_ptr(val)
        , val_sz(valsz)
        , partition_hash(hash)
        , autocommit_marker(marker) {
    }

    ~producer_user_data() {
      if (key_ptr)
        free(key_ptr);
      if (val_ptr)
        free(val_ptr);

      key_ptr = nullptr;
      val_ptr = nullptr;
    }

    uint32_t                                         partition_hash;
    std::shared_ptr<commit_chain::autocommit_marker> autocommit_marker;
    void*                                            key_ptr;
    size_t                                           key_sz;
    void*                                            val_ptr;
    size_t                                           val_sz;
  };

  int32_t kafka_producer::MyHashPartitionerCb::partitioner_cb(const RdKafka::Topic *topic, const std::string *key, int32_t partition_cnt, void *msg_opaque) {
    producer_user_data*  extra = (producer_user_data*) msg_opaque;
    int32_t partition = extra->partition_hash % partition_cnt;
    return partition;
  }

  kafka_producer::MyDeliveryReportCb::MyDeliveryReportCb() :
      _status(RdKafka::ErrorCode::ERR_NO_ERROR) {}

  void kafka_producer::MyDeliveryReportCb::dr_cb(RdKafka::Message& message) {
    producer_user_data* extra = (producer_user_data*) message.msg_opaque();

    if (message.err() != RdKafka::ErrorCode::ERR_NO_ERROR) {
      extra->autocommit_marker->fail(message.err());
      _status = message.err();
    }
    assert(extra);
    delete extra;
  }

  kafka_producer::kafka_producer(std::shared_ptr<cluster_config> cconfig, std::string topic)
      : _topic(topic)
      , _msg_cnt(0)
      , _msg_bytes(0)
      , _closed(false)
      , _nr_of_partitions(0) {
    /*
    * Create configuration objects
    */
    std::unique_ptr<RdKafka::Conf> conf(RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL));
    std::unique_ptr<RdKafka::Conf> tconf(RdKafka::Conf::create(RdKafka::Conf::CONF_TOPIC));

    /*
    * Set configuration properties
    */
    try {
      set_broker_config(conf.get(), cconfig);

      set_config(conf.get(), "dr_cb", &_delivery_report_cb);
      set_config(conf.get(), "api.version.request", "true");
      set_config(conf.get(), "queue.buffering.max.ms", std::to_string(cconfig->get_producer_buffering_time().count()));
      set_config(conf.get(), "socket.blocking.max.ms", std::to_string(cconfig->get_producer_buffering_time().count()));
      set_config(conf.get(), "socket.nagle.disable", "true");
      set_config(conf.get(), "socket.max.fails", "1000000");
      set_config(conf.get(), "message.send.max.retries", "1000000");
      set_config(tconf.get(), "partitioner_cb", &_default_partitioner);
      set_config(conf.get(), "default_topic_conf", tconf.get());
    }
    catch (std::invalid_argument& e) {
      LOG(FATAL) << "topic:" << _topic << ", bad config " << e.what();
      exit(1);
    }

    /*
    * Create producer using accumulated global configuration.
    */
    std::string errstr;
    _producer = std::unique_ptr<RdKafka::Producer>(RdKafka::Producer::create(conf.get(), errstr));
    if (!_producer) {
      LOG(FATAL) << "topic:" << _topic << ", failed to create producer:" << errstr;
      exit(1);
    }

    // we have to keep this around otherwise the paritioner does not work...
    _rd_topic = std::unique_ptr<RdKafka::Topic>(RdKafka::Topic::create(_producer.get(), _topic, tconf.get(), errstr));

    if (!_rd_topic) {
      LOG(FATAL) << "topic:" << _topic << ", failed to create topic: " << errstr;
      exit(1);
    }

    // really try to make sure the partition exist and all partitions has leaders before we continue
    RdKafka::Metadata* md = NULL;
    auto  _rd_topic = std::unique_ptr<RdKafka::Topic>(RdKafka::Topic::create(_producer.get(), _topic, nullptr, errstr));
    int64_t nr_available = 0;
    while (_nr_of_partitions == 0 || nr_available != _nr_of_partitions) {
      auto ec = _producer->metadata(false, _rd_topic.get(), &md, 5000);
      if (ec == 0) {
        const RdKafka::Metadata::TopicMetadataVector* v = md->topics();
        for (auto&& i : *v) {
          auto partitions = i->partitions();
          _nr_of_partitions = partitions->size();
          nr_available = 0;
          for (auto&& j : *partitions) {
            if ((j->err() == 0) && (j->leader() >= 0)) {
              ++nr_available;
            }
          }
        }
      }
      if (_nr_of_partitions == 0 || nr_available != _nr_of_partitions) {
        LOG(WARNING) << "topic:" << _topic << ", waiting for partitions leader to be available";
        std::this_thread::sleep_for(1s);
      }
    }
    delete md;

    LOG(INFO) << "topic:" << _topic << ", kafka producer created";
  }

  kafka_producer::~kafka_producer() {
    if (!_closed)
      close();
  }

  void kafka_producer::close() {
    if (_closed)
      return;
    _closed = true;

    if (_producer && _producer->outq_len() > 0) {
      LOG(INFO) << "topic:" << _topic << ", kafka producer closing - waiting for " << _producer->outq_len() << " messages to be written...";
    }

    // quick flush then exit anyway
    auto ec = _producer->flush(2000);
    if (ec) {
      LOG(INFO) << "topic:" << _topic << ", kafka producer flush did not finish in 2 sec " << RdKafka::err2str(ec);
    }

    // should we still keep trying here???
    /*
    while (_producer && _producer->outq_len() > 0) {
      auto ec = _producer->flush(1000);
      LOG_INFO("closing") << ", still waiting for " << _producer->outq_len() << " messages to be written... " << RdKafka::err2str(ec);
    }
    */

    _rd_topic = nullptr;
    _producer = nullptr;
    LOG(INFO) << "topic:" << _topic << ", kafka producer closed - produced " << _msg_cnt << " messages (" << _msg_bytes << " bytes)";
  }


  int kafka_producer::produce(uint32_t partition_hash, memory_management_mode mode, void* key, size_t keysz, void* value, size_t valuesz, int64_t timestamp, std::shared_ptr<commit_chain::autocommit_marker> autocommit_marker) {
    producer_user_data* user_data = nullptr;
    if (mode == kafka_producer::COPY) {
      void* pkey = malloc(keysz);
      memcpy(pkey, key, keysz);
      key = pkey;

      void* pval = malloc(valuesz);
      memcpy(pval, value, valuesz);
      value = pval;
    }
    user_data = new producer_user_data(key, keysz, value, valuesz, partition_hash, autocommit_marker);

    RdKafka::ErrorCode ec = _producer->produce(_topic, -1, 0, value, valuesz, key, keysz, timestamp, user_data); // note not using _rd_topic anymore...?
    if (ec == RdKafka::ERR__QUEUE_FULL) {
      DLOG(INFO) << "kafka_producer, topic:" << _topic << ", queue full - retrying, msg_count (" << _msg_cnt << ")";
      delete user_data;
      return ec;
    }  else if (ec != RdKafka::ERR_NO_ERROR) {
      LOG(ERROR) << "kafka_producer, topic:" << _topic << ", produce failed: " << RdKafka::err2str(ec);
      // should this be a fatal?
      delete user_data; // how do we signal failure to send data... the consumer should probably not continue...
      return ec;
    }

    _msg_cnt++;
    _msg_bytes += (valuesz + keysz);
    return 0;
  }
} // namespace