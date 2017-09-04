#include <kspp/impl/rd_kafka_utils.h>

void set_config(RdKafka::Conf* conf, std::string key, std::string value) {
  std::string errstr;
  if (conf->set(key, value, errstr) != RdKafka::Conf::CONF_OK) {
    throw std::invalid_argument("\"" + key + "\" -> " + value + ", error: " + errstr);
  }
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
  set_config(rd_conf, "bootstrap.servers", cluster_config->get_brokers());

  // experimental code only
  if (cluster_config->get_brokers().substr(0, 3) == "ssl") {
    // SSL no auth - always
    set_config(rd_conf, "security.protocol", "ssl");
    set_config(rd_conf, "ssl.ca.location", cluster_config->get_ca_cert_path());

    //client cert
    set_config(rd_conf, "ssl.certificate.location", cluster_config->get_client_cert_path());
    set_config(rd_conf, "ssl.key.location", cluster_config->get_private_key_path());
    // optional password
    if (cluster_config->get_private_key_passphrase().size())
      set_config(rd_conf, "ssl.key.password", cluster_config->get_private_key_passphrase());
  }
}



