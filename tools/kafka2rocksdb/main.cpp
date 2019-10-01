#include <iostream>
#include <csignal>
#include <boost/program_options.hpp>
#include <kspp/processors/filter.h>
#include <kspp/sources/kafka_source.h>
#include <kspp/topology_builder.h>
#include <kspp/processors/ktable.h>
#include <kspp/state_stores/rocksdb_store.h>
#include <kspp/utils/env.h>
#include <kspp/utils/kafka_utils.h>
#include <kspp/metrics/prometheus_pushgateway_reporter.h>
#include <kspp/utils/string_utils.h>

#define SERVICE_NAME "kafka2rocksdb"
#define DEFAULT_PATH "/tmp/kafka2rocksdb"

using namespace std::chrono_literals;
using namespace kspp;

static bool run = true;
static void sigterm(int sig) {
  run = false;
}

int main(int argc, char** argv) {
  FLAGS_logtostderr = 1;
  google::InitGoogleLogging(argv[0]);

  boost::program_options::options_description desc("options");
  desc.add_options()
      ("help", "produce help message")
      ("app_realm", boost::program_options::value<std::string>()->default_value(get_env_and_log("APP_REALM", "DEV")), "app_realm")
      ("src_topic", boost::program_options::value<std::string>()->default_value(get_env_and_log("SRC_TOPIC", "dummy")), "src_topic")
      ("start_offset", boost::program_options::value<std::string>()->default_value("OFFSET_STORED"), "start_offset")
      ("partition_list", boost::program_options::value<std::string>()->default_value("[-1]"), "partition_list")
      ("dst_path", boost::program_options::value<std::string>()->default_value(get_env_and_log("DST_PATH", DEFAULT_PATH)), "dst_path")
      ("consumer_group", boost::program_options::value<std::string>()->default_value(get_env_and_log("CONSUMER_GROUP", "")), "consumer_group")
      ("metrics_namespace", boost::program_options::value<std::string>()->default_value(get_env_and_log("METRICS_NAMESPACE", "bb")),"metrics_namespace")
      ;

  boost::program_options::variables_map vm;
  boost::program_options::store(boost::program_options::parse_command_line(argc, argv, desc), vm);
  boost::program_options::notify(vm);

  if (vm.count("help")) {
    std::cout << desc << std::endl;
    return 0;
  }

  std::string consumer_group;
  if (vm.count("consumer_group")) {
    consumer_group = vm["consumer_group"].as<std::string>();
  }

  auto config = std::make_shared<kspp::cluster_config>(consumer_group);
  config->load_config_from_env();

  std::string app_realm;
  if (vm.count("app_realm")) {
    app_realm = vm["app_realm"].as<std::string>();
  }

  std::string src_topic;
  if (vm.count("src_topic")) {
    src_topic = vm["src_topic"].as<std::string>();
  }

  kspp::start_offset_t start_offset=kspp::OFFSET_BEGINNING;
  try {
    if (vm.count("start_offset"))
      start_offset = kspp::to_offset(vm["start_offset"].as<std::string>());
  }
  catch(std::exception& e) {
    std::cerr << "start_offset must be one of OFFSET_BEGINNING / OFFSET_END / OFFSET_STORED";
    return -1;
  }

  std::string dst_path;
  if (vm.count("dst_path")) {
    dst_path = vm["dst_path"].as<std::string>();
  }

  std::vector<int> partition_list;
  if (vm.count("partition_list")) {
    auto s = vm["partition_list"].as<std::string>();
    partition_list = kspp::parse_partition_list(s);
  }

  std::string metrics_namespace;
  if (vm.count("metrics_namespace")) {
    metrics_namespace = vm["metrics_namespace"].as<std::string>();
  }

  config->set_producer_buffering_time(1000ms);
  config->set_consumer_buffering_time(500ms);
  config->set_storage_root(dst_path);
  config->validate();
  config->log();

  LOG(INFO) << "src_topic         : " << src_topic;
  LOG(INFO) << "start_offset      : " << kspp::to_string(start_offset);
  LOG(INFO) << "dst_path          : " << dst_path;
  LOG(INFO) << "pushgateway_uri   : " << config->get_pushgateway_uri();
  LOG(INFO) << "metrics_namespace : " << metrics_namespace;
  LOG(INFO) << "discovering facts...";

  auto nr_of_partitions = kspp::kafka::get_number_partitions(config, src_topic);
  if (partition_list.size() == 0 || partition_list[0] == -1)
    partition_list = kspp::get_partition_list(nr_of_partitions);
  LOG(INFO) << "partition_list   : " << kspp::partition_list_to_string(partition_list);

  kspp::topology_builder generic_builder(config);
  auto topology = generic_builder.create_topology();
  auto sources = topology->create_processors<kspp::kafka_source<kspp::generic_avro, kspp::generic_avro, kspp::avro_serdes, kspp::avro_serdes>>(partition_list, src_topic, config->avro_serdes(), config->avro_serdes());
  auto ktables = topology->create_processors<kspp::ktable<kspp::generic_avro, kspp::generic_avro, kspp::rocksdb_store, kspp::avro_serdes>>(sources, config->avro_serdes());

  std::signal(SIGINT, sigterm);
  std::signal(SIGTERM, sigterm);
  std::signal(SIGPIPE, SIG_IGN);

  topology->add_labels( {
                            { "app_name", SERVICE_NAME },
                            { "app_realm", app_realm },
                            { "hostname", default_hostname() },
                            { "src_topic", src_topic },
                            { "dst_path", dst_path }
                        });

  topology->start(start_offset);

  LOG(INFO) << "status is up";

  // output metrics and run...
  {
    auto metrics_reporter = std::make_shared<kspp::prometheus_pushgateway_reporter>(metrics_namespace, config->get_pushgateway_uri()) << topology;
    while (run) {
      if (topology->process(kspp::milliseconds_since_epoch()) == 0) {
        std::this_thread::sleep_for(10ms);
        topology->commit(false);
      }
      topology->commit(false);

      if (topology->eof()) {
        LOG(INFO) << "eof() - exiting";
        break;
      }
    }
  }

  LOG(INFO) << "status is down";
  topology->commit(true);
  topology->close();
  return 0;
}