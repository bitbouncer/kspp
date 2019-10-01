#include <iostream>
#include <csignal>
#include <boost/program_options.hpp>
#include <kspp/topology_builder.h>
#include <kspp/sources/kafka_source.h>
#include <kspp/utils/env.h>
#include <kspp/utils/kafka_utils.h>
#include <kspp/connect/elasticsearch/elasticsearch_generic_avro_sink.h>
#include <kspp/processors/transform.h>
#include <kspp/metrics/prometheus_pushgateway_reporter.h>
#include <kspp/utils/string_utils.h>

#define SERVICE_NAME "elasticsearch_sink"

using namespace std::chrono_literals;
using namespace kspp;

static bool run = true;

static void sigterm(int sig) {
  run = false;
}

int main(int argc, char **argv) {
  FLAGS_logtostderr = 1;
  google::InitGoogleLogging(argv[0]);

  boost::program_options::options_description desc("options");
  desc.add_options()
      ("help", "produce help message")
      ("app_realm", boost::program_options::value<std::string>()->default_value(get_env_and_log("APP_REALM", "DEV")), "app_realm")
      ("partition_list", boost::program_options::value<std::string>()->default_value("[-1]"), "partition_list")
      ("start_offset", boost::program_options::value<std::string>()->default_value("OFFSET_BEGINNING"), "start_offset")
      ("topic", boost::program_options::value<std::string>(), "topic")
      ("remote_write_uri", boost::program_options::value<std::string>()->default_value(get_env_and_log("REMOTE_WRITE_URI", "")), "remote_write_uri")
      ("remote_write_user", boost::program_options::value<std::string>()->default_value(get_env_and_log("REMOTE_WRITE_USER", "")), "remote_write_uri")
      ("remote_write_password", boost::program_options::value<std::string>()->default_value(get_env_and_log_hidden("REMOTE_WRITE_PASSWORD", "")), "remote_write_password")
      //("es_http_header", boost::program_options::value<std::string>()->default_value(get_env_and_log("ES_HTTP_HEADER")),"es_http_header")
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

  std::string topic;
  if (vm.count("topic")) {
    topic = vm["topic"].as<std::string>();
  } else {
    std::cout << "--topic must be specified" << std::endl;
    return 0;
  }

  std::vector<int> partition_list;
  if (vm.count("partition_list")) {
    auto s = vm["partition_list"].as<std::string>();
    partition_list = kspp::parse_partition_list(s);
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

  std::string remote_write_uri;
  if (vm.count("remote_write_uri")) {
    remote_write_uri = vm["remote_write_uri"].as<std::string>();
  }

  if (remote_write_uri.size()==0) {
    std::cerr << "--remote_write_uri must specified" << std::endl;
    return -1;
  }

  std::string remote_write_user;
  if (vm.count("remote_write_user")) {
    remote_write_user = vm["remote_write_user"].as<std::string>();
  }

  std::string remote_write_password;
  if (vm.count("remote_write_password")) {
    remote_write_password = vm["remote_write_password"].as<std::string>();
  }

  std::string metrics_namespace;
  if (vm.count("metrics_namespace")) {
    metrics_namespace = vm["metrics_namespace"].as<std::string>();
  }

  config->set_producer_buffering_time(1000ms);
  config->set_consumer_buffering_time(500ms);
  config->validate();
  config->log();
  auto s= config->avro_serdes();

  LOG(INFO) << "app_realm            : " << app_realm;
  LOG(INFO) << "topic                : " << topic;
  LOG(INFO) << "start_offset         : " << kspp::to_string(start_offset);


  LOG(INFO) << "remote_write_uri     : " << remote_write_uri;
  if (remote_write_user.size()) {
    LOG(INFO) << "remote_write_user    : " << remote_write_user;
    LOG(INFO) << "remote_write_password: [hidden]";
  } else {
    LOG(INFO) << "authentication       : NONE";
  }
  LOG(INFO) << "pushgateway_uri      : " << config->get_pushgateway_uri();
  LOG(INFO) << "metrics_namespace    : " << metrics_namespace;
  LOG(INFO) << "discovering facts...";

  auto nr_of_partitions = kspp::kafka::get_number_partitions(config, topic);
  if (partition_list.size() == 0 || partition_list[0] == -1)
    partition_list = kspp::get_partition_list(nr_of_partitions);
  LOG(INFO) << "partition_list   : " << kspp::partition_list_to_string(partition_list);

  kspp::topology_builder builder(config);
  auto topology = builder.create_topology();

  auto source0 = topology->create_processors<kspp::kafka_source<kspp::generic_avro, kspp::generic_avro, kspp::avro_serdes, kspp::avro_serdes>>(partition_list, topic, config->avro_serdes(), config->avro_serdes());

  topology->create_sink<kspp::elasticsearch_generic_avro_sink>(source0, remote_write_uri, remote_write_user, remote_write_password);

  topology->add_labels( {
                            { "app_name", SERVICE_NAME },
                            { "app_realm", app_realm },
                            { "hostname", default_hostname() },
                            { "remote_write_uri", remote_write_uri },
                        });

  topology->start(start_offset);

  std::signal(SIGINT, sigterm);
  std::signal(SIGTERM, sigterm);
  std::signal(SIGPIPE, SIG_IGN);

  LOG(INFO) << "status is up";

  {
    auto metrics_reporter = std::make_shared<kspp::prometheus_pushgateway_reporter>(metrics_namespace, config->get_pushgateway_uri()) << topology;
    while (run) {
      if (topology->process(kspp::milliseconds_since_epoch()) == 0) {
        std::this_thread::sleep_for(10ms);
        topology->commit(false);
      }
    }
  }

  topology->commit(true);
  topology->close();
  LOG(INFO) << "status is down";

  return 0;
}
