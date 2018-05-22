#include <iostream>
#include <csignal>
#include <boost/program_options.hpp>
#include <kspp/processors/filter.h>
#include <kspp/sources/kafka_source.h>
#include <kspp/connect/influxdb/influx_sink.h>
#include <kspp/metrics/influx_metrics_reporter.h>
#include <kspp/utils/env.h>
#include <kspp/utils/kafka_utils.h>


#define SERVICE_NAME "metrics2influx"

#define DEFAULT_METRICS_TOPIC "kspp_metric"
#define DEFAULT_URI           "http://10.1.46.22:8086"

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
      ("broker", boost::program_options::value<std::string>()->default_value(default_kafka_broker_uri()), "broker")
      ("src_topic", boost::program_options::value<std::string>()->default_value(DEFAULT_METRICS_TOPIC), "src_topic")
      ("partition_list", boost::program_options::value<std::string>()->default_value("[-1]"), "partition_list")
      ("dst_uri", boost::program_options::value<std::string>()->default_value(DEFAULT_URI), "dst_uri")
      ("schema_registry", boost::program_options::value<std::string>()->default_value(default_schema_registry_uri()), "schema_registry")
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

  std::string app_realm;
  if (vm.count("app_realm")) {
    app_realm = vm["app_realm"].as<std::string>();
  }

  std::string src_topic;
  if (vm.count("src_topic")) {
    src_topic = vm["src_topic"].as<std::string>();
  }

  std::string dst_uri;
  if (vm.count("dst_uri")) {
    dst_uri = vm["dst_uri"].as<std::string>();
  }

  int32_t http_batch_size;
  if (vm.count("http_batch_size")) {
    http_batch_size = vm["http_batch_size"].as<int32_t>();
  }

  std::chrono::milliseconds http_timeout;
  if (vm.count("http_timeout_ms")) {
    http_timeout = std::chrono::milliseconds(vm["http_timeout_ms"].as<int32_t>());
  }

  std::string broker;
  if (vm.count("broker")) {
    broker = vm["broker"].as<std::string>();
  }

  std::string schema_registry;
  if (vm.count("schema_registry")) {
    schema_registry = vm["schema_registry"].as<std::string>();
  }

  std::vector<int> partition_list;
  if (vm.count("partition_list")) {
    auto s = vm["partition_list"].as<std::string>();
    partition_list = kspp::parse_partition_list(s);
  }

  auto config = std::make_shared<kspp::cluster_config>();
  config->set_brokers(broker);
  config->set_schema_registry_uri(schema_registry);
  config->set_producer_buffering_time(1000ms);
  config->set_consumer_buffering_time(500ms);
  config->set_ca_cert_path(kspp::default_ca_cert_path());
  config->set_private_key_path(kspp::default_client_cert_path(),
                               kspp::default_client_key_path(),
                               kspp::default_client_key_passphrase());
  config->validate();
  config->log();

  LOG(INFO) << "src_topic        : " << src_topic;
  LOG(INFO) << "dst_uri          : " << dst_uri;
  LOG(INFO) << "http_batch_size  : " << http_batch_size;
  LOG(INFO) << "http_timeout_ms  : " << http_timeout.count();
  LOG(INFO) << "discovering facts...";

  //auto avro_schema_registry = std::make_shared<kspp::avro_schema_registry>(config);
  //auto avro_serdes = std::make_shared<kspp::avro_serdes>(avro_schema_registry);

  auto nr_of_partitions = kspp::kafka::get_number_partitions(config, src_topic);
  if (partition_list.size() == 0 || partition_list[0] == -1)
    partition_list = kspp::get_partition_list(nr_of_partitions);
  LOG(INFO) << "partition_list   : " << kspp::partition_list_to_string(partition_list);

  kspp::topology_builder generic_builder("kspp", SERVICE_NAME, config);
  auto topology = generic_builder.create_topology();
  auto sources = topology->create_processors<kafka_source<void, std::string, void, text_serdes>>(partition_list, src_topic);
  auto sink   = topology->create_sink<influx_sink>(sources, dst_uri, http_batch_size, http_timeout);

  std::signal(SIGINT, sigterm);
  std::signal(SIGTERM, sigterm);
  std::signal(SIGPIPE, SIG_IGN);

  std::vector<metrics20::avro::metrics20_key_tags_t> tags;
  tags.push_back(kspp::make_metrics_tag("app_name", SERVICE_NAME));
  tags.push_back(kspp::make_metrics_tag("app_realm", app_realm));
  tags.push_back(kspp::make_metrics_tag("hostname",  default_hostname()));
  tags.push_back(kspp::make_metrics_tag("src_topic", src_topic));
  tags.push_back(kspp::make_metrics_tag("dst_uri",   dst_uri));
  topology->init_metrics(tags);

  topology->start(kspp::OFFSET_STORED);

  LOG(INFO) << "status is up";

  // output metrics and run...
  {
    auto metrics_reporter = std::make_shared<kspp::influx_metrics_reporter>(generic_builder, "kspp_metrics", "kspp", "") << topology;
    while (run) {
      if (topology->process(kspp::milliseconds_since_epoch()) == 0) {
        std::this_thread::sleep_for(10ms);
        topology->commit(false);
      }
    }
  }

  topology->commit(true);
  topology->close();
  return 0;
}