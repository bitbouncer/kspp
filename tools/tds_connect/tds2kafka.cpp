#include <iostream>
#include <csignal>
#include <boost/program_options.hpp>
#include <kspp/topology_builder.h>
#include <kspp/sinks/kafka_sink.h>
#include <kspp/utils/env.h>
#include <kspp/connect/tds/tds_connection.h>
#include <kspp/connect/tds/tds_generic_avro_source.h>
#include <kspp/processors/transform.h>
#include <clocale>
#include <algorithm>
#include <kspp/metrics/prometheus_pushgateway_reporter.h>
#include <kspp/utils/string_utils.h>

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
      ("broker", boost::program_options::value<std::string>(), "broker")
      ("schema_registry", boost::program_options::value<std::string>(), "schema_registry")
      ("db_host", boost::program_options::value<std::string>()->default_value(get_env_and_log("DB_HOST")), "db_host")
      ("db_port", boost::program_options::value<int32_t>()->default_value(1433), "db_port")
      ("db_user", boost::program_options::value<std::string>()->default_value(get_env_and_log("DB_USER")), "db_user")
      ("db_password", boost::program_options::value<std::string>()->default_value(get_env_and_log("DB_PASSWORD")), "db_password")
      ("db_dbname", boost::program_options::value<std::string>()->default_value(get_env_and_log("DB_DBNAME")), "db_dbname")
      ("id_column", boost::program_options::value<std::string>()->default_value(""), "id_column")
      ("timestamp_column", boost::program_options::value<std::string>()->default_value("ts"), "timestamp_column")
      ("timestamp_unit", boost::program_options::value<std::string>(), "timestamp_unit")
      ("table", boost::program_options::value<std::string>(), "table")
      ("query", boost::program_options::value<std::string>(), "query")
      ("poll_intervall", boost::program_options::value<int32_t>()->default_value(60), "poll_intervall")
      ("rescrape", boost::program_options::value<int32_t>()->default_value(10), "rescrape")
      ("topic_prefix", boost::program_options::value<std::string>()->default_value(get_env_and_log("TOPIC_PREFIX", "DEV_sqlserver_")), "topic_prefix")
      ("topic", boost::program_options::value<std::string>(), "topic")
      ("start_offset", boost::program_options::value<std::string>()->default_value("OFFSET_BEGINNING"), "start_offset")
      ("state_store_root", boost::program_options::value<std::string>(), "state_store_root")
      ("offset_storage", boost::program_options::value<std::string>(), "offset_storage")
      ("filename", boost::program_options::value<std::string>(), "filename")
      ("pushgateway_uri", boost::program_options::value<std::string>()->default_value(get_env_and_log("PUSHGATEWAY_URI", "localhost:9091")),"pushgateway_uri")
      ("metrics_namespace", boost::program_options::value<std::string>()->default_value(get_env_and_log("METRICS_NAMESPACE", "bb")),"metrics_namespace")
      ("oneshot", "run to eof and exit")
      ;

  boost::program_options::variables_map vm;
  boost::program_options::store(boost::program_options::parse_command_line(argc, argv, desc), vm);
  boost::program_options::notify(vm);

  if (vm.count("help")) {
    std::cout << desc << std::endl;
    return 0;
  }

  std::string consumer_group(SERVICE_NAME);
  auto config = std::make_shared<kspp::cluster_config>(consumer_group);
  config->load_config_from_env();

  if (vm.count("state_store_root")) {
    config->set_storage_root(vm["state_store_root"].as<std::string>());
  }

  std::string app_realm;
  if (vm.count("app_realm")) {
    app_realm = vm["app_realm"].as<std::string>();
  }

  if (vm.count("broker")) {
    config->set_brokers(vm["broker"].as<std::string>());
  }

  if (vm.count("schema_registry")) {
    config->set_schema_registry_uri(vm["schema_registry"].as<std::string>());
  }

  std::string db_host;
  if (vm.count("db_host")) {
    db_host = vm["db_host"].as<std::string>();
  }

  int db_port;
  if (vm.count("db_port")) {
    db_port = vm["db_port"].as<int>();
  }

  int poll_intervall;
  if (vm.count("poll_intervall")) {
    poll_intervall = vm["poll_intervall"].as<int>();
  }

  int rescrape=0;
  if (vm.count("rescrape")) {
    rescrape = vm["rescrape"].as<int>();
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

  int timetamp_multiplier = 0;
  if (vm.count("timestamp_unit")) {
    auto s = vm["timestamp_unit"].as<std::string>();
    if (s == "s")
      timetamp_multiplier = 1000;
    if (s == "ms")
      timetamp_multiplier = 1;
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

  std::string offset_storage;
  if (vm.count("offset_storage")) {
    offset_storage = vm["offset_storage"].as<std::string>();
  } else {
    offset_storage = config->get_storage_root() + "/import-" + topic + ".offset";
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

  std::string pushgateway_uri;
  if (vm.count("pushgateway_uri")) {
    pushgateway_uri = vm["pushgateway_uri"].as<std::string>();
  }

  std::string metrics_namespace;
  if (vm.count("metrics_namespace")) {
    metrics_namespace = vm["metrics_namespace"].as<std::string>();
  }

  bool oneshot=false;
  if (vm.count("oneshot"))
    oneshot=true;

  config->set_producer_buffering_time(1000ms);
  config->set_consumer_buffering_time(500ms);
  config->validate();
  config->log();
  auto s= config->avro_serdes();

  LOG(INFO) << "app_realm         : " << app_realm;
  LOG(INFO) << "db_host           : " << db_host;
  LOG(INFO) << "db_port           : " << db_port;
  LOG(INFO) << "db_dbname         : " << db_dbname;
  LOG(INFO) << "db_user           : " << db_user;
  LOG(INFO) << "db_password       : " << "[hidden]";
  LOG(INFO) << "table             : " << table;
  LOG(INFO) << "query             : " << query;
  LOG(INFO) << "id_column         : " << id_column;
  LOG(INFO) << "timestamp_column  : " << timestamp_column;
  LOG(INFO) << "poll_intervall    : " << poll_intervall;
  LOG(INFO) << "rescrape          : " << rescrape;
  LOG(INFO) << "topic_prefix      : " << topic_prefix;
  LOG(INFO) << "topic             : " << topic;
  LOG(INFO) << "offset_storage    : " << offset_storage;
  LOG(INFO) << "start_offset      : " << kspp::to_string(start_offset);
  LOG(INFO) << "pushgateway_uri   : " << pushgateway_uri;
  LOG(INFO) << "metrics_namespace : " << metrics_namespace;
  if (oneshot)
    LOG(INFO) << "oneshot           : TRUE";

  kspp::connect::connection_params connection_params;
  connection_params.host = db_host;
  connection_params.port = db_port;
  connection_params.user = db_user;
  connection_params.password = db_password;
  connection_params.database_name = db_dbname;

  kspp::connect::table_params table_params;
  table_params.poll_intervall = std::chrono::seconds(poll_intervall);
  table_params.rescrape_policy = kspp::connect::LAST_QUERY_TS;
  table_params.rescrape_ticks = rescrape;
  table_params.offset_storage = offset_storage;
  table_params.ts_utc_offset=0;
  table_params.ts_multiplier=timetamp_multiplier;

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

  topology->add_labels( {
                            { "app_name", SERVICE_NAME },
                            { "app_realm", app_realm },
                            { "hostname", default_hostname() },
                            { "db_host", db_host }
                        });
  if (query.size()>0)
    topology->add_labels( { { "source", "query" } }); // TODO check if we can use arbitrary query as value here
  else
    topology->add_labels( { { "source", table } });

  topology->start(start_offset);

  std::signal(SIGINT, sigterm);
  std::signal(SIGTERM, sigterm);
  std::signal(SIGPIPE, SIG_IGN);

  LOG(INFO) << "status is up";

  // output metrics and run
  {
    auto metrics_reporter = std::make_shared<kspp::prometheus_pushgateway_reporter>(metrics_namespace, pushgateway_uri) << topology;
    while (run) {
      if (topology->process(kspp::milliseconds_since_epoch()) == 0) {
        std::this_thread::sleep_for(10ms);
        topology->commit(false);
        if (oneshot && topology->eof()){
          LOG(INFO) << "at eof - flushing";
          topology->flush(true);
          LOG(INFO) << "at eof - exiting";
          break;
        }
      }
    }
  }

  topology->commit(true);
  topology->close();
  LOG(INFO) << "status is down";
  return 0;
}
