#include <iostream>
#include <chrono>
#include <kspp/utils/env.h>
#include <glog/logging.h>
//#include <kspp/utils/kspp_utils.h>
#include <kspp/impl/kafka_utils.h>
#include <kspp/cluster_config.h>

using namespace std::chrono_literals;


int main(int argc, char **argv) {
  FLAGS_logtostderr = 1;
  google::InitGoogleLogging(argv[0]);

  auto config = std::make_shared<kspp::cluster_config>();
  config->set_brokers(kspp::default_kafka_broker_uri());
  config->set_ca_cert_path(kspp::default_ca_cert_path());
  config->set_private_key_path(kspp::default_client_cert_path(),
                               kspp::default_client_key_path(),
                               kspp::default_client_key_passphrase());
  config->set_schema_registry(kspp::default_schema_registry_uri());
  config->validate(); // optional
  config->log(); // optional

  auto partitions1 = kspp::kafka::get_number_partitions(config, "kspp_test0_eventstream"); // just a random partition
  std::cout << partitions1 << std::endl;

  return 0;
}
