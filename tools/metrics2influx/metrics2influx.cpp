#ifdef WIN32
#define BOOST_ASIO_ERROR_CATEGORY_NOEXCEPT noexcept(true)
#endif

#include <iostream>
#include <csignal>
#include <boost/program_options.hpp>
#include <kspp/processors/filter.h>
#include <kspp/sources/kafka_source.h>
#include <kspp/sinks/kafka_sink.h>
#include <kspp/metrics/influx_metrics_reporter.h>
#include <kspp/utils/env.h>
#include "influx_sink.h"

#define SERVICE_NAME "metrics2influx"
#define DEFAULT_METRICS_TOPIC "vast_metrics"

using namespace std::chrono_literals;

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
      ("partition_list", boost::program_options::value<std::string>()->default_value("[-1]"), "partition_list")
      ("metrics_topic", boost::program_options::value<std::string>()->default_value(DEFAULT_METRICS_TOPIC), "metrics_topic")
      ("broker", boost::program_options::value<std::string>()->default_value(kspp::default_kafka_broker_uri()), "broker")
      ("schema_registry", boost::program_options::value<std::string>()->default_value(kspp::default_schema_registry_uri()), "schema_registry")
      ("http_batch_size", boost::program_options::value<int32_t>()->default_value(500), "http_batch_size")
      ("http_timeout_ms", boost::program_options::value<int32_t>()->default_value(1000), "http_timeout_ms")
      ;

  boost::program_options::variables_map vm;
  boost::program_options::store(boost::program_options::parse_command_line(argc, argv, desc), vm);
  boost::program_options::notify(vm);

  if (vm.count("help")) {
    std::cout << desc << std::endl;
    return 0;
  }

  std::string metrics_topic;
  if (vm.count("metrics_topic")) {
    metrics_topic = vm["metrics_topic"].as<std::string>();
  } else {
    std::cout << "--metrics_topic must be specified" << std::endl;
    return 0;
  }

  /*std::string metrics_tags;
  if (vm.count("metrics_tags")) {
    metrics_tags = vm["metrics_tags"].as<std::string>();
  } else {
    std::cout << "--metrics_tags must be specified" << std::endl;
    return 0;
  }
   */

  int32_t http_batch_size;
  if (vm.count("http_batch_size")) {
    http_batch_size = vm["http_batch_size"].as<int32_t>();
  } else {
    std::cout << "--http_batch_size must be specified" << std::endl;
    return 0;
  }

  std::chrono::milliseconds http_timeout;
  if (vm.count("http_timeout_ms")) {
    http_timeout = std::chrono::milliseconds(vm["http_timeout_ms"].as<int32_t>());
  } else {
    std::cout << "--http_timeout_ms must be specified" << std::endl;
    return 0;
  }

  std::string broker;
  if (vm.count("broker")) {
    broker = vm["broker"].as<std::string>();
  } else {
    std::cout << "--broker must be specified" << std::endl;
    return 0;
  }

  std::string schema_registry;
  if (vm.count("schema_registry")) {
    schema_registry = vm["schema_registry"].as<std::string>();
  } else {
    std::cout << "--schema_registry must be specified" << std::endl;
    return 0;
  }

  std::vector<int> partition_list;
  if (vm.count("partition_list")) {
    auto s = vm["partition_list"].as<std::string>();
    partition_list = kspp::parse_partition_list(s);
  } else {
    std::cout << "--partition_list must be specified" << std::endl;
    return 0;
  }

  auto config = std::make_shared<kspp::cluster_config>();
  config->set_brokers(broker);
  config->set_schema_registry(schema_registry);
  config->set_producer_buffering_time(1000ms);
  config->set_consumer_buffering_time(500ms);
  config->set_ca_cert_path(kspp::default_ca_cert_path());
  config->set_private_key_path(kspp::default_client_cert_path(),
                               kspp::default_client_key_path(),
                               kspp::default_client_key_passphrase());
  config->validate();
  config->log();

  LOG(INFO) << "metrics_topic    : " << metrics_topic;
  LOG(INFO) << "http_batch_size  : " << http_batch_size;
  LOG(INFO) << "http_timeout_ms  : " << http_timeout.count();

  LOG(INFO) << "discovering facts...";

  //auto avro_schema_registry = std::make_shared<kspp::avro_schema_registry>(config);
  //auto avro_serdes = std::make_shared<kspp::avro_serdes>(avro_schema_registry);

  auto nr_of_partitions = kspp::kafka::get_number_partitions(config, metrics_topic);
  if (partition_list.size() == 0 || partition_list[0] == -1)
    partition_list = kspp::get_partition_list(nr_of_partitions);
  LOG(INFO) << "partition_list   : " << kspp::partition_list_to_string(partition_list);

  kspp::topology_builder generic_builder("kspp", SERVICE_NAME, config);
  auto topology = generic_builder.create_topology();
  auto sources = topology->create_processors<kspp::kafka_source<void, std::string, kspp::text_serdes>>(partition_list, metrics_topic);
  auto sink   = topology->create_sink<influx_sink>(sources, "base url", http_batch_size, http_timeout);
  signal(SIGINT, sigterm);
  signal(SIGTERM, sigterm);

  //topology->init_metrics();
  topology->start(kspp::OFFSET_STORED);
  topology->init();

  // output metrics and run...
  {
    //auto metrics_reporter = std::make_shared<kspp::influx_metrics_reporter>(generic_builder, metrics_topic, "kspp", "") << topology;
    while (run) {
      if (topology->process_one() == 0) {
        std::this_thread::sleep_for(10ms);
        topology->commit(false);
      }
    }
  }
  topology->commit(true);
  topology->close();
  return 0;
}