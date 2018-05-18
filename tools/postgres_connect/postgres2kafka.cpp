#include <iostream>
#include <csignal>
#include <boost/program_options.hpp>
#include <kspp/sinks/kafka_sink.h>
#include <kspp/metrics/influx_metrics_reporter.h>
#include <kspp/utils/env.h>
#include <boost/bind.hpp>
#include <boost/make_shared.hpp>
#include <kspp/connect/postgres/postgres_generic_avro_source.h>
#include <kspp/connect/avro_file/avro_file_sink.h>
#include <kspp/processors/transform.h>
#include <kspp/processors/flat_map.h>

#define SERVICE_NAME "postgres2kafka"

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
      ("broker", boost::program_options::value<std::string>()->default_value(kspp::default_kafka_broker_uri()), "broker")
      ("schema_registry", boost::program_options::value<std::string>()->default_value(kspp::default_schema_registry_uri()), "schema_registry")
      ("app_realm", boost::program_options::value<std::string>()->default_value(get_env_and_log("APP_REALM", "DEV")), "app_realm")
      ("postgres_host", boost::program_options::value<std::string>()->default_value(get_env_and_log("POSTGRES_HOST")), "postgres_host")
      ("postgres_port", boost::program_options::value<int32_t>()->default_value(5432), "postgres_port")
      ("postgres_user", boost::program_options::value<std::string>()->default_value(get_env_and_log("POSTGRES_USER")), "postgres_user")
      ("postgres_password", boost::program_options::value<std::string>()->default_value(get_env_and_log_hidden("POSTGRES_PASSWORD")), "postgres_password")
      ("postgres_dbname", boost::program_options::value<std::string>()->default_value(get_env_and_log("POSTGRES_DBNAME")), "postgres_dbname")
      ("postgres_max_items_in_fetch", boost::program_options::value<int32_t>()->default_value(1000), "postgres_max_items_in_fetch")
      ("postgres_warning_timeout", boost::program_options::value<int32_t>()->default_value(1000), "postgres_warning_timeout")
      ("postgres_table", boost::program_options::value<std::string>()->default_value(get_env_and_log("POSTGRES_TABLE")), "postgres_table")
      ("topic_prefix", boost::program_options::value<std::string>()->default_value("JUNK_postgres_"), "topic_prefix")
      ("topic_name_override", boost::program_options::value<std::string>(), "topic_name_override")
      ("filename", boost::program_options::value<std::string>(), "filename");

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

  std::string app_realm;
  if (vm.count("app_realm")) {
    app_realm = vm["app_realm"].as<std::string>();
  } else {
    std::cout << "--app_realm must be specified" << std::endl;
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

  std::string postgres_table;
  if (vm.count("postgres_table")) {
    postgres_table = vm["postgres_table"].as<std::string>();
  } else {
    std::cout << "--postgres_table must be specified" << std::endl;
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

  std::string topic_prefix;
  if (vm.count("topic_prefix")) {
    topic_prefix = vm["topic_prefix"].as<std::string>();
  } else {
    std::cout << "--topic_prefix must be specified" << std::endl;
    return 0;
  }

  std::string topic_name_override;
  if (vm.count("topic_name_override")) {
    topic_name_override = vm["topic_name_override"].as<std::string>();
  } else {
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

  std::string topic_name = topic_prefix + postgres_table;

  if (topic_name_override.size())
    topic_name=topic_name_override;

  LOG(INFO) << "app_realm                   : " << app_realm;
  LOG(INFO) << "postgres_host               : " << postgres_host;
  LOG(INFO) << "postgres_port               : " << postgres_port;
  LOG(INFO) << "postgres_dbname             : " << postgres_dbname;
  LOG(INFO) << "postgres_table              : " << postgres_table;
  LOG(INFO) << "postgres_user               : " << postgres_user;
  LOG(INFO) << "postgres_password           : " << "[hidden]";
  LOG(INFO) << "postgres_max_items_in_fetch : " << postgres_max_items_in_fetch;
  LOG(INFO) << "postgres_warning_timeout    : " << postgres_warning_timeout;
  LOG(INFO) << "topic_prefix                : " << topic_prefix;
  LOG(INFO) << "topic_name_override         : " << topic_name_override;
  LOG(INFO) << "topic_name                  : " << topic_name;


  std::string connect_string =
      "host=" + postgres_host + " port=" + std::to_string(postgres_port) + " user=" + postgres_user + " password=" +
      postgres_password + " dbname=" + postgres_dbname;

  if (filename.size()) {
     LOG(INFO) << "using avro file..";
    LOG(INFO) << "filename                   : " << filename;
  }

  LOG(INFO) << "discovering facts...";

  kspp::topology_builder generic_builder("kspp", SERVICE_NAME, config);
  auto topology = generic_builder.create_topology();
  auto source0 = topology->create_processors<kspp::postgres_generic_avro_source>({0}, postgres_table, connect_string, "id", "", config->get_schema_registry(), 60s);
  if (filename.size()) {
    topology->create_sink<kspp::avro_file_sink>(source0, "/tmp/" + topic_name + ".avro");
  } else {
    topology->create_sink<kspp::kafka_sink<void, kspp::GenericAvro, void, kspp::avro_serdes>>(source0, topic_name, config->avro_serdes());
  }


  std::vector<metrics20::avro::metrics20_key_tags_t> tags;
  tags.push_back(kspp::make_metrics_tag("app_name", SERVICE_NAME));
  tags.push_back(kspp::make_metrics_tag("app_realm", app_realm));
  tags.push_back(kspp::make_metrics_tag("hostname", default_hostname()));
  tags.push_back(kspp::make_metrics_tag("db_host", postgres_host));
  tags.push_back(kspp::make_metrics_tag("dst_topic", topic_name));


  //auto sink   = topology->create_sink<kspp::kafka_sink<void, kspp::GenericAvro, void, kspp::avro_serdes>>(source, topic_prefix + postgres_table, config->avro_serdes());

  //topology->init_metrics();
  //topology->start(kspp::OFFSET_STORED);
  //topology->init();


  //signal(SIGINT, sigterm);
  //signal(SIGTERM, sigterm);

  // output metrics and run...
  {
    //auto metrics_reporter = std::make_shared<kspp::influx_metrics_reporter>(generic_builder, metrics_topic, "kspp", "") << topology;
    /*while (run) {
      if (topology->process_one() == 0) {
        std::this_thread::sleep_for(10ms);
        topology->commit(false);
      }
    }
     */
  }

  topology->start(kspp::OFFSET_BEGINNING);
  topology->flush();

  topology->commit(true);
  topology->close();

  return 0;
}
