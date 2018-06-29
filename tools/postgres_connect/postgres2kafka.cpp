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
      ("db_host", boost::program_options::value<std::string>()->default_value(get_env_and_log("DB_HOST")), "db_host")
      ("db_port", boost::program_options::value<int32_t>()->default_value(5432), "db_port")
      ("db_user", boost::program_options::value<std::string>()->default_value(get_env_and_log("DB_USER")), "db_user")
      ("db_password", boost::program_options::value<std::string>()->default_value(get_env_and_log_hidden("DB_PASSWORD")), "db_password")
      ("db_dbname", boost::program_options::value<std::string>()->default_value(get_env_and_log("DB_DBNAME")), "db_dbname")
      ("id_column", boost::program_options::value<std::string>()->default_value(""), "id_column")
      ("timestamp_column", boost::program_options::value<std::string>()->default_value("updated_at"), "timestamp_column")
      ("poll_intervall", boost::program_options::value<int32_t>()->default_value(10), "poll_intervall")
      ("max_items_in_fetch", boost::program_options::value<int32_t>()->default_value(1000), "max_items_in_fetch")
      ("warning_timeout", boost::program_options::value<int32_t>()->default_value(1000), "warning_timeout")
      ("table", boost::program_options::value<std::string>()->default_value(get_env_and_log("TABLE")), "table")
      ("topic_prefix", boost::program_options::value<std::string>()->default_value("DEV_postgres_"), "topic_prefix")
      ("topic", boost::program_options::value<std::string>(), "topic")
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
  }

  std::string db_host;
  if (vm.count("db_host")) {
    db_host = vm["db_host"].as<std::string>();
  }

  int db_port;
  if (vm.count("db_port")) {
    db_port = vm["db_port"].as<int>();
  }

  std::string db_dbname;
  if (vm.count("db_dbname")) {
    db_dbname = vm["db_dbname"].as<std::string>();
  }

  std::string db_user;
  if (vm.count("db_user")) {
    db_user = vm["db_user"].as<std::string>();
  }

  std::string db_password;
  if (vm.count("db_password")) {
    db_password = vm["db_password"].as<std::string>();
  }

  std::string id_column;
  if (vm.count("id_column")) {
    id_column = vm["id_column"].as<std::string>();
  }

  std::string timestamp_column;
  if (vm.count("timestamp_column")) {
    timestamp_column = vm["timestamp_column"].as<std::string>();
  }

  int max_items_in_fetch;
  if (vm.count("max_items_in_fetch")) {
    max_items_in_fetch = vm["max_items_in_fetch"].as<int>();
  }

  int warning_timeout;
  if (vm.count("warning_timeout")) {
    warning_timeout = vm["warning_timeout"].as<int>();
  }

  int poll_intervall;
  if (vm.count("poll_intervall")) {
    poll_intervall = vm["poll_intervall"].as<int>();
  }

  std::string table;
  if (vm.count("table")) {
    table = vm["table"].as<std::string>();
  }

  std::string topic_prefix;
  if (vm.count("topic_prefix")) {
    topic_prefix = vm["topic_prefix"].as<std::string>();
  }

  std::string topic;
  if (vm.count("topic")) {
    topic = vm["topic"].as<std::string>();
  } else {
    topic = topic_prefix + table;
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

  LOG(INFO) << "app_realm          : " << app_realm;
  LOG(INFO) << "table              : " << table;
  LOG(INFO) << "db_host            : " << db_host;
  LOG(INFO) << "db_port            : " << db_port;
  LOG(INFO) << "db_user            : " << db_user;
  LOG(INFO) << "db_password        : " << "[hidden]";
  LOG(INFO) << "db_dbname          : " << db_dbname;
  LOG(INFO) << "max_items_in_fetch : " << max_items_in_fetch;
  LOG(INFO) << "warning_timeout    : " << warning_timeout;
  LOG(INFO) << "topic_prefix       : " << topic_prefix;
  LOG(INFO) << "topic              : " << topic;
  LOG(INFO) << "id_column          : " << id_column;
  LOG(INFO) << "timestamp_column   : " << timestamp_column;
  LOG(INFO) << "poll_intervall     : " << poll_intervall;

  kspp::connect::connection_params connection_params;
  connection_params.host = db_host;
  connection_params.port = db_port;
  connection_params.user = db_user;
  connection_params.password = db_password;
  connection_params.database = db_dbname;

  if (filename.size()) {
     LOG(INFO) << "using avro file..";
    LOG(INFO) << "filename                   : " << filename;
  }

  LOG(INFO) << "discovering facts...";

  kspp::topology_builder generic_builder("kspp", SERVICE_NAME, config);
  auto topology = generic_builder.create_topology();
  auto source0 = topology->create_processors<kspp::postgres_generic_avro_source>({0}, table, connection_params, id_column, timestamp_column, config->get_schema_registry(), std::chrono::seconds(poll_intervall), max_items_in_fetch);

  if (filename.size()) {
    //topology->create_sink<kspp::avro_file_sink>(source0, "/tmp/" + topic + ".avro");
  } else {
    topology->create_sink<kspp::kafka_sink< kspp::generic_avro, kspp::generic_avro, kspp::avro_serdes, kspp::avro_serdes>>(source0, topic, config->avro_serdes(), config->avro_serdes());
  }

  std::vector<metrics20::avro::metrics20_key_tags_t> tags;
  tags.push_back(kspp::make_metrics_tag("app_name", SERVICE_NAME));
  tags.push_back(kspp::make_metrics_tag("app_realm", app_realm));
  tags.push_back(kspp::make_metrics_tag("hostname", default_hostname()));
  tags.push_back(kspp::make_metrics_tag("db_host", db_host));
  tags.push_back(kspp::make_metrics_tag("dst_topic", topic));


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
