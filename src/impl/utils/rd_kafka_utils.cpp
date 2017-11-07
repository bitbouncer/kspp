#include <kspp/impl/rd_kafka_utils.h>
#include <thread>
#include <chrono>
#include <glog/logging.h>
#include <kspp/utils/url_parser.h>

using namespace std::chrono_literals;

void set_config(RdKafka::Conf* conf, std::string key, std::string value) {
  std::string errstr;
  if (conf->set(key, value, errstr) != RdKafka::Conf::CONF_OK) {
    throw std::invalid_argument("\"" + key + "\" -> " + value + ", error: " + errstr);
  }
  DLOG(INFO) << "rd_kafka set_config: " << key << "->" << value;
}

void set_config(RdKafka::Conf* conf, std::string key, RdKafka::Conf* topic_conf) {
  std::string errstr;
  if (conf->set(key, topic_conf, errstr) != RdKafka::Conf::CONF_OK) {
    throw std::invalid_argument("\"" + key + ", error: " + errstr);
  }
}

void set_config(RdKafka::Conf* conf, std::string key, RdKafka::DeliveryReportCb* callback) {
  std::string errstr;
  if (conf->set(key, callback, errstr) != RdKafka::Conf::CONF_OK) {
    throw std::invalid_argument("\"" + key + "\", error: " + errstr);
  }
}

void set_config(RdKafka::Conf* conf, std::string key, RdKafka::PartitionerCb* partitioner_cb) {
  std::string errstr;
  if (conf->set(key, partitioner_cb, errstr) != RdKafka::Conf::CONF_OK) {
    throw std::invalid_argument("\"" + key + "\", error: " + errstr);
  }
}

void set_config(RdKafka::Conf* conf, std::string key, RdKafka::EventCb* event_cb) {
  std::string errstr;
  if (conf->set(key, event_cb, errstr) != RdKafka::Conf::CONF_OK) {
    throw std::invalid_argument("\"" + key + "\", error: " + errstr);
  }
}

void set_broker_config(RdKafka::Conf* rd_conf, std::shared_ptr<kspp::cluster_config> cluster_config) {
  auto v = kspp::split_url_list(cluster_config->get_brokers());

  set_config(rd_conf, "bootstrap.servers", cluster_config->get_brokers());

  if ((v.size()>0) && v[0].scheme() == "ssl" )
  {
    // SSL no auth - always
    set_config(rd_conf, "security.protocol", "ssl");
    set_config(rd_conf, "ssl.ca.location", cluster_config->get_ca_cert_path());

    //do we have client certs
    if (cluster_config->get_client_cert_path().size()>0 && cluster_config->get_private_key_path().size()>0) {
      set_config(rd_conf, "ssl.certificate.location", cluster_config->get_client_cert_path());
      set_config(rd_conf, "ssl.key.location", cluster_config->get_private_key_path());
      // optional password
      if (cluster_config->get_private_key_passphrase().size())
        set_config(rd_conf, "ssl.key.password", cluster_config->get_private_key_passphrase());
    }
  }
}

/*
int wait_for_topic(RdKafka::Handle *handle, std::string topic) {
  std::string errstr;
  // really try to make sure the partition exist before we continue
  RdKafka::Metadata *md = NULL;
  auto _rd_topic = std::unique_ptr<RdKafka::Topic>(RdKafka::Topic::create(handle, topic, nullptr, errstr));
  int64_t nr_available = 0;
  int64_t nr_of_partitions = 0;
  while (nr_of_partitions == 0 || nr_available != nr_of_partitions) {
    auto ec = handle->metadata(false, _rd_topic.get(), &md, 5000);
    if (ec == 0) {
      const RdKafka::Metadata::TopicMetadataVector *v = md->topics();
      for (auto &&i : *v) {
        auto partitions = i->partitions();
        nr_of_partitions = partitions->size();
        nr_available = 0;
        for (auto &&j : *partitions) {
          if ((j->err() == 0) && (j->leader() >= 0)) {
            ++nr_available;
          }
        }
      }
    }
    if (nr_of_partitions == 0 || nr_available != nr_of_partitions) {
      LOG(ERROR) << ", waiting for partitions leader to be available, " << topic;
      std::this_thread::sleep_for(1s);
    }
  }
  delete md;
  return 0;
}
 */

int wait_for_partition(RdKafka::Handle *handle, std::string topic, int32_t partition) {
  LOG_IF(FATAL, partition<0);

  std::string errstr;
  // make sure the partition exist before we continue
  RdKafka::Metadata *md = NULL;
  auto _rd_topic = std::unique_ptr<RdKafka::Topic>(RdKafka::Topic::create(handle, topic, nullptr, errstr));
  bool is_available = false;
  while (!is_available) {
    auto ec = handle->metadata(false, _rd_topic.get(), &md, 5000);
    if (ec == 0) {
      const RdKafka::Metadata::TopicMetadataVector *v = md->topics();
      for (auto &&i : *v) {
        auto partitions = i->partitions();
        for (auto &&j : *partitions) {
          if ((j->err() == 0) && (j->id() == partition) && (j->leader() >= 0)) {
            is_available = true;
            break;
          }
        }
      }
    }
    if (!is_available) {
      LOG(ERROR) << ", waiting for partitions leader to be available, " << topic << ":" << partition;
      std::this_thread::sleep_for(1s);
    }
  }
  delete md;
  return 0;
}




