#include <iostream>
#include <chrono>
#include <kspp/utils/env.h>
#include <glog/logging.h>
#include <kspp/utils/kafka_utils.h>
#include <kspp/cluster_config.h>

using namespace std::chrono_literals;


int main(int argc, char **argv) {
  FLAGS_logtostderr = 1;
  google::InitGoogleLogging(argv[0]);

  auto config = std::make_shared<kspp::cluster_config>();
  config->load_config_from_env();
  config->validate(); // optional
  config->log(); // optional

  std::string topicname = argc>1 ? argv[1] : "kspp_test0_eventstream";  // just a random topic

  auto partitions1 = kspp::kafka::get_number_partitions(config, topicname);
  std::cout << partitions1 << std::endl;

  return 0;
}
