#include <kspp/impl/rd_kafka_utils.h>
#include <glog/logging.h>
#include <kspp/utils/url_parser.h>

void set_config(RdKafka::Conf* conf, std::string key, std::string value) {
  std::string errstr;
  if (conf->set(key, value, errstr) != RdKafka::Conf::CONF_OK) {
    throw std::invalid_argument("\"" + key + "\" -> " + value + ", error: " + errstr);
  }
  LOG(INFO) << "rd_kafka set_config: " << key << "->" << value;
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



