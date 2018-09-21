#include <iostream>
#include <csignal>
#include <boost/program_options.hpp>
#include <kspp/sinks/kafka_sink.h>
#include <kspp/metrics/influx_metrics_reporter.h>
#include <kspp/utils/env.h>
#include <boost/bind.hpp>
#include <boost/make_shared.hpp>
#include <kspp/utils/env.h>
#include <kspp/connect/tds/tds_connection.h>
#include <kspp/connect/tds/tds_generic_avro_source.h>
#include <kspp/connect/avro_file/avro_file_sink.h>
#include <kspp/processors/transform.h>
#include <kspp/processors/flat_map.h>
#include <clocale>

#define SERVICE_NAME "tds2kafka"

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
      ("db_host", boost::program_options::value<std::string>()->default_value(get_env_and_log("DB_HOST")), "db_host")
      ("db_port", boost::program_options::value<int32_t>()->default_value(1433), "db_port")
      ("db_user", boost::program_options::value<std::string>()->default_value(get_env_and_log("DB_USER")), "db_user")
      ("db_password", boost::program_options::value<std::string>()->default_value(get_env_and_log("DB_PASSWORD")), "db_password")
      ("db_dbname", boost::program_options::value<std::string>()->default_value(get_env_and_log("DB_DBNAME")), "db_dbname")
      ("id_column", boost::program_options::value<std::string>()->default_value(""), "id_column")
      ("poll_algorithm", boost::program_options::value<std::string>()->default_value("MUTABLE"), "poll_algorithm")
      ("timestamp_column", boost::program_options::value<std::string>()->default_value("ts"), "timestamp_column")
      ("table", boost::program_options::value<std::string>(), "table")
      ("query", boost::program_options::value<std::string>(), "query")
      ("poll_intervall", boost::program_options::value<int32_t>()->default_value(60), "poll_intervall")
      ("topic_prefix", boost::program_options::value<std::string>()->default_value(get_env_and_log("TOPIC_PREFIX", "DEV_sqlserver_")), "topic_prefix")
      ("topic", boost::program_options::value<std::string>(), "topic")
      ("filename", boost::program_options::value<std::string>(), "filename");

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

  std::string db_host;
  if (vm.count("db_host")) {
    db_host = vm["db_host"].as<std::string>();
  }

  int db_port;
  if (vm.count("db_port")) {
    db_port = vm["db_port"].as<int>();
  }

  kspp::connect::row_constness_t  row_constness = kspp::connect::MUTABLE;
  std::string poll_algorithm;
  if (vm.count("poll_algorithm")) {
    poll_algorithm = vm["poll_algorithm"].as<std::string>();
    if (poll_algorithm == "MUTABLE"){
      row_constness = kspp::connect::MUTABLE;
    } else if (poll_algorithm == "IMMUTABLE") {
      row_constness = kspp::connect::IMMUTABLE;
    } else {
      std::cerr <<  "poll_algorith must be MUTABLE/IMMUTABLE";
    }
  }

  int poll_intervall;
  if (vm.count("poll_intervall")) {
    poll_intervall = vm["poll_intervall"].as<int>();
  }

  std::string db_dbname;
  if (vm.count("db_dbname")) {
    db_dbname = vm["db_dbname"].as<std::string>();
  }

  std::string id_column;
  if (vm.count("id_column")) {
    id_column = vm["id_column"].as<std::string>();
  }

  std::string timestamp_column;
  if (vm.count("timestamp_column")) {
    timestamp_column = vm["timestamp_column"].as<std::string>();
  }

  std::string table;
  if (vm.count("table")) {
    table = vm["table"].as<std::string>();
  }

  std::string query;
  if (vm.count("query")) {
    query = vm["query"].as<std::string>();
  }

  if (table.size()==0 && query.size()==0){
    std::cerr << "--table or --query must be specified";
    return -1;
  }

  if (query.size()==0){
    query = "SELECT * FROM " + table;
  } else {
  }

  std::string db_user;
  if (vm.count("db_user")) {
    db_user = vm["db_user"].as<std::string>();
  }

  std::string db_password;
  if (vm.count("db_password")) {
    db_password = vm["db_password"].as<std::string>();
  }

  std::string filename;
  if (vm.count("filename")) {
    filename = vm["filename"].as<std::string>();
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

  std::string consumer_group(SERVICE_NAME);
  auto config = std::make_shared<kspp::cluster_config>(consumer_group);
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

  LOG(INFO) << "app_realm         : " << app_realm;
  LOG(INFO) << "db_host           : " << db_host;
  LOG(INFO) << "db_port           : " << db_port;
  LOG(INFO) << "db_dbname         : " << db_dbname;
  LOG(INFO) << "db_user           : " << db_user;
  LOG(INFO) << "db_password       : " << "[hidden]";
  LOG(INFO) << "table              : " << table;
  LOG(INFO) << "query              : " << query;
  LOG(INFO) << "id_column         : " << id_column;
  LOG(INFO) << "timestamp_column  : " << timestamp_column;
  LOG(INFO) << "poll_algorithm    : " << to_string(row_constness);
  LOG(INFO) << "poll_intervall    : " << poll_intervall;
  LOG(INFO) << "topic_prefix      : " << topic_prefix;
  LOG(INFO) << "topic             : " << topic;

  kspp::connect::connection_params connection_params;
  connection_params.host = db_host;
  connection_params.port = db_port;
  connection_params.user = db_user;
  connection_params.password = db_password;
  connection_params.database = db_dbname;

  kspp::connect::table_params table_params;
  table_params.row_constness = row_constness;
  table_params.poll_intervall = std::chrono::seconds(poll_intervall);

  if (filename.size()) {
     LOG(INFO) << "using avro file..";
    LOG(INFO) << "filename                   : " << filename;
  }

  LOG(INFO) << "discovering facts...";

  std::setlocale(LC_ALL, "en_US.UTF-8");

  kspp::topology_builder builder(config);
  auto topology = builder.create_topology();

  std::string query_name = topic;
  auto source0 = topology->create_processors<kspp::tds_generic_avro_source>({0}, query_name, connection_params, table_params, query, id_column, timestamp_column, config->get_schema_registry());

   if (filename.size()) {
    //topology->create_sink<kspp::avro_file_sink>(source0, "/tmp/" + topic + ".avro");
  } else {
    topology->create_sink<kspp::kafka_sink<kspp::generic_avro, kspp::generic_avro, kspp::avro_serdes, kspp::avro_serdes>>(source0, topic, config->avro_serdes(), config->avro_serdes());
  }

  std::vector<metrics20::avro::metrics20_key_tags_t> tags;
  tags.push_back(kspp::make_metrics_tag("app_name", SERVICE_NAME));
  tags.push_back(kspp::make_metrics_tag("app_realm", app_realm));
  tags.push_back(kspp::make_metrics_tag("hostname", default_hostname()));
  tags.push_back(kspp::make_metrics_tag("db_host", db_host));

  if (query.size()>0)
    tags.push_back(kspp::make_metrics_tag("source", "query"));
  else
    tags.push_back(kspp::make_metrics_tag("source", table));

  topology->init_metrics(tags);
  //topology->start(kspp::OFFSET_STORED);
  topology->start(kspp::OFFSET_BEGINNING);

  std::signal(SIGINT, sigterm);
  std::signal(SIGTERM, sigterm);
  std::signal(SIGPIPE, SIG_IGN);

  LOG(INFO) << "status is up";


  /*
   * topology->for_each_metrics([](kspp::metric &m) {
    std::cerr << "metrics: " << m.name() << " : " << m.value() << std::endl;
  });
   */


  // output metrics and run
  {
    auto metrics_reporter = std::make_shared<kspp::influx_metrics_reporter>(builder, "kspp_metrics", "kspp", "") << topology;
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
