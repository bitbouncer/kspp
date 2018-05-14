#include <iostream>
#include <csignal>
#include <boost/program_options.hpp>
#include <kspp/sources/kafka_source.h>
#include <kspp/metrics/influx_metrics_reporter.h>
#include <kspp/utils/env.h>
#include <kspp/utils/kafka_utils.h>
#include <boost/bind.hpp>
#include <boost/make_shared.hpp>
#include <kspp/connect/postgres/postgres_generic_avro_sink.h>
#include <kspp/connect/avro_file/avro_file_sink.h>
#include <kspp/processors/transform.h>
#include <kspp/processors/flat_map.h>

#define SERVICE_NAME "kafka2postgres"

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
      ("postgres_host", boost::program_options::value<std::string>()->default_value(get_env("POSTGRES_HOST")),
       "postgres_host")
      ("postgres_port", boost::program_options::value<int32_t>()->default_value(5432), "postgres_port")
      ("postgres_user", boost::program_options::value<std::string>()->default_value(get_env("POSTGRES_USER")),
       "postgres_user")
      ("postgres_password", boost::program_options::value<std::string>()->default_value(get_env("POSTGRES_PASSWORD")),
       "postgres_password")
      ("postgres_dbname", boost::program_options::value<std::string>()->default_value(get_env("POSTGRES_DBNAME")),
       "postgres_dbname")
      ("postgres_max_items_in_fetch", boost::program_options::value<int32_t>()->default_value(1000),
       "postgres_max_items_in_fetch")
      ("postgres_warning_timeout", boost::program_options::value<int32_t>()->default_value(1000),
       "postgres_warning_timeout")
      ("table_prefix", boost::program_options::value<std::string>()->default_value("kafka_"), "table_prefix")
      ("character_encoding", boost::program_options::value<std::string>()->default_value("UTF8"), "character_encoding")
      ("filename", boost::program_options::value<std::string>(), "filename");

  boost::program_options::variables_map vm;
  boost::program_options::store(boost::program_options::parse_command_line(argc, argv, desc), vm);
  boost::program_options::notify(vm);

  if (vm.count("help")) {
    std::cout << desc << std::endl;
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

  std::string postgres_host;
  if (vm.count("postgres_host")) {
    postgres_host = vm["postgres_host"].as<std::string>();
  } else {
    std::cout << "--postgres_host must be specified" << std::endl;
    return 0;
  }

  int postgres_port;
  if (vm.count("postgres_port")) {
    postgres_port = vm["postgres_port"].as<int>();
  } else {
    std::cout << "--postgres_port must be specified" << std::endl;
    return 0;
  }

  std::string postgres_dbname;
  if (vm.count("postgres_dbname")) {
    postgres_dbname = vm["postgres_dbname"].as<std::string>();
  } else {
    std::cout << "--postgres_dbname must be specified" << std::endl;
    return 0;
  }

  std::string postgres_user;
  if (vm.count("postgres_user")) {
    postgres_user = vm["postgres_user"].as<std::string>();
  } else {
    std::cout << "--postgres_user must be specified" << std::endl;
    return 0;
  }

  std::string postgres_password;
  if (vm.count("postgres_password")) {
    postgres_password = vm["postgres_password"].as<std::string>();
  } else {
    std::cout << "--postgres_password must be specified" << std::endl;
    return 0;
  }

  int postgres_max_items_in_fetch;
  if (vm.count("postgres_max_items_in_fetch")) {
    postgres_max_items_in_fetch = vm["postgres_max_items_in_fetch"].as<int>();
  } else {
    std::cout << "--postgres_max_items_in_fetch must be specified" << std::endl;
    return 0;
  }

  int postgres_warning_timeout;
  if (vm.count("postgres_warning_timeout")) {
    postgres_warning_timeout = vm["postgres_warning_timeout"].as<int>();
  } else {
    std::cout << "--postgres_warning_timeout must be specified" << std::endl;
    return 0;
  }

  std::string table_prefix;
  if (vm.count("table_prefix")) {
    table_prefix = vm["table_prefix"].as<std::string>();
  } else {
    std::cout << "--table_prefix must be specified" << std::endl;
    return 0;
  }

  std::string character_encoding;
  if (vm.count("character_encoding")) {
    character_encoding = vm["character_encoding"].as<std::string>();
  } else {
    std::cout << "--character_encoding must be specified" << std::endl;
    return 0;
  }

  std::string filename;
  if (vm.count("filename")) {
    filename = vm["filename"].as<std::string>();
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

  LOG(INFO) << "topic                       : " << topic;

  LOG(INFO) << "postgres_host               : " << postgres_host;
  LOG(INFO) << "postgres_port               : " << postgres_port;
  LOG(INFO) << "postgres_dbname             : " << postgres_dbname;
  LOG(INFO) << "postgres_user               : " << postgres_user;
  LOG(INFO) << "postgres_password           : " << "[hidden]";
  LOG(INFO) << "postgres_max_items_in_fetch : " << postgres_max_items_in_fetch;
  LOG(INFO) << "postgres_warning_timeout    : " << postgres_warning_timeout;
  LOG(INFO) << "table_prefix                : " << table_prefix;
  LOG(INFO) << "character_encoding          : " << character_encoding;

  std::string connect_string =
      "host=" + postgres_host + " port=" + std::to_string(postgres_port) + " user=" + postgres_user + " password=" +
      postgres_password + " dbname=" + postgres_dbname;

  if (filename.size()) {
     LOG(INFO) << "using avro file..";
    LOG(INFO) << "filename                   : " << filename;
  }

  LOG(INFO) << "discovering facts...";


  auto nr_of_partitions = kspp::kafka::get_number_partitions(config, topic);
  if (partition_list.size() == 0 || partition_list[0] == -1)
    partition_list = kspp::get_partition_list(nr_of_partitions);
  LOG(INFO) << "partition_list   : " << kspp::partition_list_to_string(partition_list);

  kspp::topology_builder generic_builder("kspp", SERVICE_NAME, config);
  auto topology = generic_builder.create_topology();

  auto source0 = topology->create_processors<kspp::kafka_source<void, kspp::GenericAvro, void, kspp::avro_serdes>>(partition_list, topic, config->avro_serdes());

  /*https://www.postgresql.org/docs/9.3/static/multibyte.html*/

  if (filename.size()) {
    topology->create_sink<kspp::avro_file_sink>(source0, "/tmp/" + table_prefix + topic + ".avro");
  } else {
    topology->create_sink<kspp::postgres_generic_avro_sink>(source0, table_prefix + topic, connect_string, "id", config->get_schema_registry(), character_encoding);
  }

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
