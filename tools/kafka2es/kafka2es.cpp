#include <iostream>
#include <csignal>
#include <boost/program_options.hpp>
#include <kspp/sources/kafka_source.h>
#include <kspp/metrics/influx_metrics_reporter.h>
#include <kspp/utils/env.h>
#include <kspp/utils/kafka_utils.h>
#include <boost/bind.hpp>
#include <boost/make_shared.hpp>
#include <kspp/connect/elasticsearch/elasticsearch_generic_avro_sink.h>
#include <kspp/processors/transform.h>
#include <kspp/processors/flat_map.h>

#define SERVICE_NAME "elasticsearch_sink"

using namespace std::chrono_literals;

static bool run = true;

static void sigterm(int sig) {
  run = false;
}

static std::string get_env(const char *env) {
  std::cerr << env << std::endl;
  const char *env_p = std::getenv(env);
  if (env_p)
    return std::string(env_p);
  return "";
}


int main(int argc, char **argv) {
  FLAGS_logtostderr = 1;
  google::InitGoogleLogging(argv[0]);

  boost::program_options::options_description desc("options");
  desc.add_options()
      ("help", "produce help message")
      ("broker", boost::program_options::value<std::string>()->default_value(kspp::default_kafka_broker_uri()),
       "broker")
      ("schema_registry",
       boost::program_options::value<std::string>()->default_value(kspp::default_schema_registry_uri()),
       "schema_registry")
      ("partition_list", boost::program_options::value<std::string>()->default_value("[-1]"), "partition_list")
      ("topic", boost::program_options::value<std::string>(), "topic")
      ("es_url", boost::program_options::value<std::string>()->default_value(get_env("ES_URL")), "es_url")
      ("es_index", boost::program_options::value<std::string>()->default_value(get_env("ES_USER")), "elasticsearch_user")
      ("es_http_header", boost::program_options::value<std::string>()->default_value(get_env("ES_HTTP_HEADER")),"es_http_header")
      ;

  boost::program_options::variables_map vm;
  boost::program_options::store(boost::program_options::parse_command_line(argc, argv, desc), vm);
  boost::program_options::notify(vm);

  if (vm.count("help")) {
    std::cout << desc << std::endl;
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
  } else {
    std::cout << "--partition_list must be specified" << std::endl;
    return 0;
  }

  std::string es_url;
  if (vm.count("es_url")) {
    es_url = vm["es_url"].as<std::string>();
  } else {
    std::cout << "--es_url must be specified" << std::endl;
    return 0;
  }

  std::string es_http_header;
  if (vm.count("es_http_header")) {
    es_http_header = vm["es_http_header"].as<std::string>();
  } else {
    std::cout << "--es_http_header must be specified" << std::endl;
    return 0;
  }

  std::string es_index;
  if (vm.count("es_index")) {
    es_index = vm["es_index"].as<std::string>();
  }  else {
    es_index = "kafka_" + topic;
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
  auto s= config->avro_serdes();

  LOG(INFO) << "topic           : " << topic;
  LOG(INFO) << "es_url          : " << es_url;
  LOG(INFO) << "es_http_header  : " << es_http_header;
  LOG(INFO) << "es_index        : " << es_index;

  /*
   *
   * std::string connect_string =
      "host=" + postgres_host + " port=" + std::to_string(postgres_port) + " user=" + postgres_user + " password=" +
      postgres_password + " dbname=" + postgres_dbname;
*/
  //std::string base_url= "http://" + elasticsearch_host + ":" + std::to_string(elasticsearch_port);

  /*
  curl -X PUT "localhost:9200/twitter/_doc/1" -H 'Content-Type: application/json' -d'
  {
    "user" : "kimchy",
        "post_date" : "2009-11-15T14:12:12",
        "message" : "trying out Elasticsearch"
  }
  '
   */

  LOG(INFO) << "discovering facts...";


  auto nr_of_partitions = kspp::kafka::get_number_partitions(config, topic);
  if (partition_list.size() == 0 || partition_list[0] == -1)
    partition_list = kspp::get_partition_list(nr_of_partitions);
  LOG(INFO) << "partition_list   : " << kspp::partition_list_to_string(partition_list);

  kspp::topology_builder generic_builder("kspp", SERVICE_NAME, config);
  auto topology = generic_builder.create_topology();

  auto source0 = topology->create_processors<kspp::kafka_source<void, kspp::GenericAvro, void, kspp::avro_serdes>>(partition_list, topic, config->avro_serdes());

  topology->create_sink<kspp::elasticsearch_generic_avro_sink>(source0, es_index, es_url, es_http_header, "id", config->get_schema_registry());


  topology->init_metrics();
  //topology->start(kspp::OFFSET_STORED);
  topology->start(kspp::OFFSET_BEGINNING);

  std::signal(SIGINT, sigterm);
  std::signal(SIGTERM, sigterm);
  std::signal(SIGPIPE, SIG_IGN);

  LOG(INFO) << "status is up";

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
