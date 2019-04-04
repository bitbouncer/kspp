#include <iostream>
#include <csignal>
#include <boost/program_options.hpp>
#include <kspp/topology_builder.h>
#include <kspp/impl/serdes/text_serdes.h>
#include <kspp/sinks/kafka_sink.h>
#include <kspp/utils/env.h>
#include <boost/bind.hpp>
#include <boost/make_shared.hpp>
#include <kspp/connect/postgres/postgres_generic_avro_source.h>
#include <kspp/connect/avro_file/avro_file_sink.h>
#include <kspp/processors/transform.h>
#include <kspp/processors/flat_map.h>
#include <kspp/metrics/prometheus_pushgateway_reporter.h>

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
      ("broker", boost::program_options::value<std::string>(), "broker")
      ("schema_registry", boost::program_options::value<std::string>(), "schema_registry")
      ("app_realm", boost::program_options::value<std::string>()->default_value(get_env_and_log("APP_REALM", "DEV")), "app_realm")
      ("db_host", boost::program_options::value<std::string>()->default_value(get_env_and_log("DB_HOST")), "db_host")
      ("db_port", boost::program_options::value<int32_t>()->default_value(5432), "db_port")
      ("db_user", boost::program_options::value<std::string>()->default_value(get_env_and_log("DB_USER")), "db_user")
      ("db_password", boost::program_options::value<std::string>()->default_value(get_env_and_log_hidden("DB_PASSWORD")), "db_password")
      ("db_dbname", boost::program_options::value<std::string>()->default_value(get_env_and_log("DB_DBNAME")), "db_dbname")
      ("id_column", boost::program_options::value<std::string>()->default_value(""), "id_column")
      ("timestamp_column", boost::program_options::value<std::string>()->default_value(""), "timestamp_column")
      ("timestamp_unit", boost::program_options::value<std::string>(), "timestamp_unit")
      ("poll_intervall", boost::program_options::value<int32_t>()->default_value(60), "poll_intervall")
      ("max_items_in_fetch", boost::program_options::value<int32_t>()->default_value(1000), "max_items_in_fetch")
      ("warning_timeout", boost::program_options::value<int32_t>()->default_value(1000), "warning_timeout")
      ("table", boost::program_options::value<std::string>(), "table")
      ("query", boost::program_options::value<std::string>(), "query")
      ("codec", boost::program_options::value<std::string>()->default_value("avro"), "codec")
      ("val_column", boost::program_options::value<std::string>()->default_value(""), "val_column")
      ("topic_prefix", boost::program_options::value<std::string>()->default_value("DEV_postgres_"), "topic_prefix")
      ("topic", boost::program_options::value<std::string>(), "topic")
      ("start_offset", boost::program_options::value<std::string>()->default_value("OFFSET_BEGINNING"), "start_offset")
      ("offset_storage", boost::program_options::value<std::string>()->default_value(""), "offset_storage")
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

  if (vm.count("broker")) {
    config->set_brokers(vm["broker"].as<std::string>());
  }

  if (vm.count("schema_registry")) {
    config->set_schema_registry_uri(vm["schema_registry"].as<std::string>());
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

  int timetamp_multiplier = 0;
  if (vm.count("timestamp_unit")) {
    auto s = vm["timestamp_unit"].as<std::string>();
    if (s == "s")
      timetamp_multiplier = 1000;
    if (s == "ms")
      timetamp_multiplier = 1;
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

  std::string query;
  if (vm.count("query")) {
    query = vm["query"].as<std::string>();
  }

  std::string codec;
  if (vm.count("codec")) {
    codec = vm["codec"].as<std::string>();

    if (codec == "avro" || codec == "text"){
      //OK
    } else {
      std::cerr << "codec must be text or avro"  << std::endl;
      return -1;
    }
  }

  std::string val_column;
  if (vm.count("val_column")) {
    val_column = vm["val_column"].as<std::string>();

    if (val_column.size()>0 && codec!="text") {
      std::cerr << "--val_column only valid for --codec=text" << std::endl;
      return -1;
    }

    if (val_column.size()==0 && codec=="text") {
      std::cerr << "--val_column required for codec=text" << std::endl;
      return -1;
    }
  }

  if (table.size()==0 && query.size()==0){
    std::cerr << "--table or --query must be specified";
    return -1;
  }

  if (query.size()==0){
    query = "SELECT * FROM " + table;
  } else {
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

  std::string offset_storage;
  if (vm.count("offset_storage")) {
    offset_storage = vm["offset_storage"].as<std::string>();
  } else {
    offset_storage = config->get_storage_root() + "/import-" + topic + ".offset";
  }

  kspp::start_offset_t start_offset=kspp::OFFSET_BEGINNING;
  if (vm.count("start_offset")) {
    auto s = vm["start_offset"].as<std::string>();
    if (boost::iequals(s, "OFFSET_BEGINNING"))
      start_offset=kspp::OFFSET_BEGINNING;
    else if (boost::iequals(s, "OFFSET_END"))
      start_offset=kspp::OFFSET_END;
    else if (boost::iequals(s, "OFFSET_STORED"))
      start_offset=kspp::OFFSET_STORED;
    else {
      std::cerr << "start_offset must be one of OFFSET_BEGINNING / OFFSET_END / OFFSET_STORED";
      return -1;
    }
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

  LOG(INFO) << "app_realm          : " << app_realm;
  LOG(INFO) << "db_host            : " << db_host;
  LOG(INFO) << "db_port            : " << db_port;
  LOG(INFO) << "db_user            : " << db_user;
  LOG(INFO) << "db_password        : " << "[hidden]";
  LOG(INFO) << "db_dbname          : " << db_dbname;
  LOG(INFO) << "max_items_in_fetch : " << max_items_in_fetch;
  LOG(INFO) << "warning_timeout    : " << warning_timeout;
  LOG(INFO) << "table              : " << table;
  LOG(INFO) << "query              : " << query;
  LOG(INFO) << "id_column          : " << id_column;
  if (val_column.size())
    LOG(INFO) << "val_column              : " << val_column;

  LOG(INFO) << "timestamp_column   : " << timestamp_column;
  LOG(INFO) << "poll_intervall     : " << poll_intervall;
  LOG(INFO) << "topic_prefix       : " << topic_prefix;
  LOG(INFO) << "topic              : " << topic;
  LOG(INFO) << "codec              : " << codec;

  LOG(INFO) << "offset_storage     : " << offset_storage;
  LOG(INFO) << "start_offset       : " << kspp::to_string(start_offset);
  LOG(INFO) << "pushgateway_uri    : " << pushgateway_uri;
  LOG(INFO) << "metrics_namespace  : " << metrics_namespace;
  if (oneshot)
    LOG(INFO) << "oneshot            : TRUE";
  kspp::connect::connection_params connection_params;
  connection_params.host = db_host;
  connection_params.port = db_port;
  connection_params.user = db_user;
  connection_params.password = db_password;
  connection_params.database_name = db_dbname;

  kspp::connect::table_params table_params;
  table_params.poll_intervall = std::chrono::seconds(poll_intervall);
  table_params.max_items_in_fetch = max_items_in_fetch;
  table_params.offset_storage = offset_storage;
  table_params.ts_multiplier=timetamp_multiplier;

  if (filename.size()) {
     LOG(INFO) << "using avro file..";
    LOG(INFO) << "filename                   : " << filename;
  }

  LOG(INFO) << "discovering facts...";

  kspp::topology_builder builder(config);
  auto topology = builder.create_topology();
  std::string query_name = topic;
  auto source0 = topology->create_processors<kspp::postgres_generic_avro_source>({0}, query_name, connection_params, table_params, query, id_column, timestamp_column, config->get_schema_registry());

  if (filename.size()) {
    //topology->create_sink<kspp::avro_file_sink>(source0, "/tmp/" + topic + ".avro");
  } else {
    if (codec == "avro"){
    topology->create_sink<kspp::kafka_sink< kspp::generic_avro, kspp::generic_avro, kspp::avro_serdes, kspp::avro_serdes>>(source0, topic, config->avro_serdes(), config->avro_serdes());
  } else if (codec == "text" ){
      auto extracted= topology->create_processors<kspp::flat_map<kspp::generic_avro, kspp::generic_avro, std::string, std::string>>(
          source0,
          [id_column, val_column](const auto record, auto flat_map) {
          std::string key = *record->key().record().get_optional_as_string(id_column);
            if (record->value()==nullptr){
              flat_map->push_back(std::make_shared<kspp::krecord<std::string, std::string>>(key, nullptr));
          //TODO
        } else {
          auto val = record->value()->record().get_optional_as_string(val_column);
          if (val)
            flat_map->push_back(std::make_shared<kspp::krecord<std::string, std::string>>(key, *val));
          else
            flat_map->push_back(std::make_shared<kspp::krecord<std::string, std::string>>(key, nullptr));
        }
      });
      topology->create_sink<kspp::kafka_sink<std::string, std::string, kspp::text_serdes, kspp::text_serdes>>(extracted, topic);
    }
  }

  topology->add_labels( {
                            { "app_name", SERVICE_NAME },
                            { "app_realm", app_realm },
                            { "hostname", default_hostname() },
                            { "db_host", db_host },
                            { "dst_topic", topic }
                        });

  topology->start(start_offset);

  std::signal(SIGINT, sigterm);
  std::signal(SIGTERM, sigterm);
  std::signal(SIGPIPE, SIG_IGN);

  LOG(INFO) << "status is up";
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
