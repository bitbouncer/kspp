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
      ("broker", boost::program_options::value<std::string>()->default_value(kspp::default_kafka_broker_uri()), "broker")
      ("schema_registry", boost::program_options::value<std::string>()->default_value(kspp::default_schema_registry_uri()), "schema_registry")
      ("partition_list", boost::program_options::value<std::string>()->default_value("[-1]"), "partition_list")
      ("topic", boost::program_options::value<std::string>(), "topic")
      ("es_url", boost::program_options::value<std::string>()->default_value(get_env_and_log("ES_URL")), "es_url")
      ("es_user", boost::program_options::value<std::string>()->default_value(get_env_and_log("ES_USER")), "es_user")
      ("es_password", boost::program_options::value<std::string>()->default_value(get_env_and_log_hidden("ES_PASSWORD")), "es_password")
      ("es_index", boost::program_options::value<std::string>(), "es_index")
      ("es_http_header", boost::program_options::value<std::string>()->default_value(get_env_and_log("ES_HTTP_HEADER")),"es_http_header")
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

  std::string broker;
  if (vm.count("broker")) {
    broker = vm["broker"].as<std::string>();
  }

  std::string schema_registry;
  if (vm.count("schema_registry")) {
    schema_registry = vm["schema_registry"].as<std::string>();
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

  std::string es_url;
  if (vm.count("es_url")) {
    es_url = vm["es_url"].as<std::string>();
  }

  std::string es_user;
  if (vm.count("es_user")) {
    es_user = vm["es_user"].as<std::string>();
  }

  std::string es_password;
  if (vm.count("es_password")) {
    es_password = vm["es_password"].as<std::string>();
  }

  std::string es_http_header;
  if (vm.count("es_http_header")) {
    es_http_header = vm["es_http_header"].as<std::string>();
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

  LOG(INFO) << "app_realm       : " << app_realm;
  LOG(INFO) << "topic           : " << topic;
  LOG(INFO) << "es_url          : " << es_url;
  LOG(INFO) << "es_user         : " << es_user;
  LOG(INFO) << "es_password     : " << "[hidden]";
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

  kspp::connect::connection_params connection_params;
  connection_params.url = es_url;
  connection_params.user = es_user;
  connection_params.password = es_password;

  auto nr_of_partitions = kspp::kafka::get_number_partitions(config, topic);
  if (partition_list.size() == 0 || partition_list[0] == -1)
    partition_list = kspp::get_partition_list(nr_of_partitions);
  LOG(INFO) << "partition_list   : " << kspp::partition_list_to_string(partition_list);

  kspp::topology_builder generic_builder("kspp", SERVICE_NAME, config);
  auto topology = generic_builder.create_topology();

  auto source0 = topology->create_processors<kspp::kafka_source<void, kspp::GenericAvro, void, kspp::avro_serdes>>(partition_list, topic, config->avro_serdes());

  topology->create_sink<kspp::elasticsearch_generic_avro_sink>(source0, es_index, connection_params, "id", config->get_schema_registry());


  std::vector<metrics20::avro::metrics20_key_tags_t> tags;
  tags.push_back(kspp::make_metrics_tag("app_name", SERVICE_NAME));
  tags.push_back(kspp::make_metrics_tag("app_realm", app_realm));
  tags.push_back(kspp::make_metrics_tag("hostname", default_hostname()));
  tags.push_back(kspp::make_metrics_tag("es_url", es_url));
  tags.push_back(kspp::make_metrics_tag("es_index", es_index));

  topology->init_metrics(tags);

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
  LOG(INFO) << "status is down";

  return 0;
}
