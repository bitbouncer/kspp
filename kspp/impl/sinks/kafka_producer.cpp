#include <thread>
#include <chrono>
#include <boost/log/trivial.hpp>
#include "kafka_producer.h"

#define LOGPREFIX_ERROR BOOST_LOG_TRIVIAL(error) << BOOST_CURRENT_FUNCTION << ", topic:" << _topic
#define LOG_INFO(EVENT)  BOOST_LOG_TRIVIAL(info) << "kafka_producer: " << EVENT << ", topic:" << _topic

using namespace std::chrono_literals;

namespace kspp {
  struct producer_user_data
  {
    producer_user_data(uint32_t hash, std::shared_ptr<commit_chain::autocommit_marker> marker)
    : partition_hash(hash)
    , autocommit_marker(marker) {
    }

    ~producer_user_data() {
    }
    uint32_t                                         partition_hash;
    std::shared_ptr<commit_chain::autocommit_marker> autocommit_marker;
  };

  int32_t kafka_producer::MyHashPartitionerCb::partitioner_cb(const RdKafka::Topic *topic, const std::string *key, int32_t partition_cnt, void *msg_opaque) {
    producer_user_data*  extra = (producer_user_data*) msg_opaque;
    int32_t partition = extra->partition_hash % partition_cnt;
    return partition;
  }

  kafka_producer::MyDeliveryReportCb::MyDeliveryReportCb() :
    _status(RdKafka::ErrorCode::ERR_NO_ERROR) {
  }

  void kafka_producer::MyDeliveryReportCb::dr_cb(RdKafka::Message& message) {
    producer_user_data* extra = (producer_user_data*) message.msg_opaque();

    if (message.err() != RdKafka::ErrorCode::ERR_NO_ERROR) {
      extra->autocommit_marker->fail(message.err());
      _status = message.err();
    }
    assert(extra);
    delete extra; 
  };

kafka_producer::kafka_producer(std::string brokers, std::string topic) 
  : _topic(topic)
  , _msg_cnt(0)
  , _msg_bytes(0)
  , _closed(false)
  , _nr_of_partitions(0) {
  std::string errstr;
  /*
  * Create configuration objects
  */
  std::unique_ptr<RdKafka::Conf> conf(RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL));

  if (conf->set("dr_cb", &_delivery_report_cb, errstr) != RdKafka::Conf::CONF_OK) {
    LOGPREFIX_ERROR << ", failed to add _delivery_report_cb " << errstr;
    exit(1);
  }

  /*
  * Set configuration properties
  */
  if (conf->set("metadata.broker.list", brokers, errstr) != RdKafka::Conf::CONF_OK) {
    LOGPREFIX_ERROR << ", failed to set metadata.broker.list " << errstr;
    exit(1);
  }

  if (conf->set("api.version.request", "true", errstr) != RdKafka::Conf::CONF_OK) {
    LOGPREFIX_ERROR << ", failed to set api.version.request " << errstr;
    exit(1);
  }

  if (conf->set("queue.buffering.max.ms", "100", errstr) != RdKafka::Conf::CONF_OK) {
    LOGPREFIX_ERROR << ", failed to set queue.buffering.max.ms " << errstr;
    exit(1);
  }

  if (conf->set("socket.blocking.max.ms", "100", errstr) != RdKafka::Conf::CONF_OK) {
    LOGPREFIX_ERROR << ", failed to set socket.blocking.max.ms " << errstr;
    exit(1);
  }

  if (conf->set("socket.nagle.disable", "true", errstr) != RdKafka::Conf::CONF_OK) {
    LOGPREFIX_ERROR << ", failed to set socket.nagle.disable=true " << errstr;
    exit(1);
  }

  /*
  * Create producer using accumulated global configuration.
  */
  _producer = std::unique_ptr<RdKafka::Producer>(RdKafka::Producer::create(conf.get(), errstr));
  if (!_producer) {
    LOGPREFIX_ERROR << ", failed to create producer:" << errstr;
    exit(1);
  }

  std::unique_ptr<RdKafka::Conf> tconf(RdKafka::Conf::create(RdKafka::Conf::CONF_TOPIC));

  if (conf->set("default_topic_conf", tconf.get(), errstr) != RdKafka::Conf::CONF_OK) {
    LOGPREFIX_ERROR << ", failed to set default_topic_conf " << errstr;
    exit(1);
  }

  if (tconf->set("partitioner_cb", &_default_partitioner, errstr) != RdKafka::Conf::CONF_OK) {
    LOGPREFIX_ERROR << ", failed to create partitioner: " << errstr;
    exit(1);
  }

  _rd_topic = std::unique_ptr<RdKafka::Topic>(RdKafka::Topic::create(_producer.get(), _topic, tconf.get(), errstr));

  if (!_rd_topic) {
    LOGPREFIX_ERROR << ", failed to create topic: " << errstr;
    exit(1);
  }

  // really try to make sure the partition exist and all partitions has leaders before we continue
  RdKafka::Metadata* md = NULL;
  auto  _rd_topic = std::unique_ptr<RdKafka::Topic>(RdKafka::Topic::create(_producer.get(), _topic, nullptr, errstr));
  int64_t nr_available = 0;
  while (_nr_of_partitions==0 || nr_available != _nr_of_partitions) {
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
    if (_nr_of_partitions==0 || nr_available != _nr_of_partitions) {
      LOGPREFIX_ERROR << ", waiting for partitions leader to be available";
      std::this_thread::sleep_for(1s);
    }
  }
  delete md;
  
  LOG_INFO("created");
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
    LOG_INFO("closing") << ", waiting for " << _producer->outq_len() << " messages to be written...";
  }

  // quick flush then exit anyway
  auto ec = _producer->flush(2000);
  if (ec) {
    LOG_INFO("closing") << ", flush did not finish in 2 sec " << RdKafka::err2str(ec);
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
  LOG_INFO("closed") << ", produced " << _msg_cnt << " messages (" << _msg_bytes << " bytes)";
}

/*
int kafka_producer::produce(uint32_t partition_hash, rdkafka_memory_management_mode mode, void* key, size_t keysz, void* value, size_t valuesz, std::shared_ptr<commit_chain::autocommit_marker> autocommit_marker) {
  auto user_data = new producer_user_data(partition_hash, autocommit_marker);
  RdKafka::ErrorCode ec = _producer->produce(_rd_topic.get(), -1, (int) mode, value, valuesz, key, keysz, user_data);
  if (ec != RdKafka::ERR_NO_ERROR) {
    LOGPREFIX_ERROR << ", Produce failed: " << RdKafka::err2str(ec);
    delete user_data; // how do we signal failure to send data... the consumer should probably not continue...
    return ec;
  }
  _msg_cnt++;
  _msg_bytes += (valuesz + keysz);
  return 0;
}
*/

int kafka_producer::produce(uint32_t partition_hash, rdkafka_memory_management_mode mode, void* key, size_t keysz, void* value, size_t valuesz, int64_t timestamp, std::shared_ptr<commit_chain::autocommit_marker> autocommit_marker) {
  auto user_data = new producer_user_data(partition_hash, autocommit_marker);
  RdKafka::ErrorCode ec = _producer->produce(_topic, -1, (int) mode, value, valuesz, key, keysz, timestamp, user_data); // note not using _rd_topic anymore...?
  if (ec != RdKafka::ERR_NO_ERROR) {
    LOGPREFIX_ERROR << ", Produce failed: " << RdKafka::err2str(ec);
    delete user_data; // how do we signal failure to send data... the consumer should probably not continue...
    return ec;
  }
  _msg_cnt++;
  _msg_bytes += (valuesz + keysz);
  return 0;
}

}; // namespace