#include <iostream>
#include <csignal>
#include <boost/program_options.hpp>
#include <kspp/utils/env.h>
#include <kspp/processors/generic_stream.h>
#include <kspp/processors/visitor.h>
#include <kspp/connect/bitbouncer/grpc_avro_source.h>
#include <kspp/connect/postgres/postgres_generic_avro_sink.h>
#include <kspp/topology_builder.h>
//#include "grpc_db_streamer.h"

#define SERVICE_NAME     "bb2pg"
#define DEFAULT_SRC_URI  "lb.bitbouncer.com:10063"

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
      ("src_uri", boost::program_options::value<std::string>()->default_value(get_env_and_log("SRC_URI", DEFAULT_SRC_URI)), "src_uri")
      ("bb_api_key", boost::program_options::value<std::string>()->default_value(get_env_and_log_hidden("BB_API_KEY", "")), "bb_api_key")
      ("bb_secret_access_key", boost::program_options::value<std::string>()->default_value(get_env_and_log_hidden("BB_SECRET_ACCESS_KEY", "")), "bb_secret_access_key")
      ("topic", boost::program_options::value<std::string>()->default_value("logs"), "topic")
      ("offset_storage", boost::program_options::value<std::string>(), "offset_storage")
      ("start_offset", boost::program_options::value<std::string>()->default_value("OFFSET_BEGINNING"), "start_offset")
      ("postgres_host", boost::program_options::value<std::string>()->default_value(get_env_and_log("POSTGRES_HOST")), "postgres_host")
      ("postgres_port", boost::program_options::value<int32_t>()->default_value(5432), "postgres_port")
      ("postgres_user", boost::program_options::value<std::string>()->default_value(get_env_and_log("POSTGRES_USER")), "postgres_user")
      ("postgres_password", boost::program_options::value<std::string>()->default_value(get_env_and_log_hidden("POSTGRES_PASSWORD")), "postgres_password")
      ("postgres_dbname", boost::program_options::value<std::string>()->default_value(get_env_and_log("POSTGRES_DBNAME")), "postgres_dbname")
      ("postgres_tablename", boost::program_options::value<std::string>()->default_value(get_env_and_log("POSTGRES_TABLENAME")), "postgres_tablename")
      ("postgres_max_items_in_insert", boost::program_options::value<int32_t>()->default_value(1000), "postgres_max_items_in_insert")
      ("postgres_warning_timeout", boost::program_options::value<int32_t>()->default_value(1000), "postgres_warning_timeout")
      ("postgres_disable_delete", boost::program_options::value<int32_t>(), "postgres_disable_delete")
      ("id_column", boost::program_options::value<std::string>()->default_value("id"), "id_column")
      ("table_prefix", boost::program_options::value<std::string>()->default_value("kafka_"), "table_prefix")
      ("character_encoding", boost::program_options::value<std::string>()->default_value("UTF8"), "character_encoding")
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
  auto config = std::make_shared<kspp::cluster_config>(consumer_group, kspp::cluster_config::NONE);
  config->load_config_from_env();

  std::string src_uri;
  if (vm.count("src_uri")) {
    src_uri = vm["src_uri"].as<std::string>();
  } else {
    std::cerr << "--src_uri must specified" << std::endl;
    return -1;
  }

  std::string bb_api_key;
  if (vm.count("bb_api_key")) {
    bb_api_key = vm["bb_api_key"].as<std::string>();
  }

  if (bb_api_key.size()==0){
    std::cerr << "--bb_api_key must be defined" << std::endl;
    return -1;
  }

  std::string bb_secret_access_key;
  if (vm.count("bb_secret_access_key")) {
    bb_secret_access_key = vm["bb_secret_access_key"].as<std::string>();
  }



  std::string offset_storage;
  if (vm.count("offset_storage")) {
    offset_storage = vm["offset_storage"].as<std::string>();
  } else {
    offset_storage = config->get_storage_root() + "/" + SERVICE_NAME + "-import-metrics.offset";
  }

  std::string topic;
  if (vm.count("topic")) {
    topic = vm["topic"].as<std::string>();
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

  std::string postgres_host;
  if (vm.count("postgres_host")) {
    postgres_host = vm["postgres_host"].as<std::string>();
  }

  int postgres_port;
  if (vm.count("postgres_port")) {
    postgres_port = vm["postgres_port"].as<int>();
  }

  std::string postgres_dbname;
  if (vm.count("postgres_dbname")) {
    postgres_dbname = vm["postgres_dbname"].as<std::string>();
  }

  std::string postgres_tablename;
  if (vm.count("postgres_tablename")) {
    postgres_tablename = vm["postgres_tablename"].as<std::string>();
  }

  std::string postgres_user;
  if (vm.count("postgres_user")) {
    postgres_user = vm["postgres_user"].as<std::string>();
  }

  std::string postgres_password;
  if (vm.count("postgres_password")) {
    postgres_password = vm["postgres_password"].as<std::string>();
  }

  int postgres_max_items_in_insert;
  if (vm.count("postgres_max_items_in_insert")) {
    postgres_max_items_in_insert = vm["postgres_max_items_in_insert"].as<int>();
  }

  int postgres_warning_timeout;
  if (vm.count("postgres_warning_timeout")) {
    postgres_warning_timeout = vm["postgres_warning_timeout"].as<int>();
  }

  std::string id_column;
  if (vm.count("id_column")) {
    id_column = vm["id_column"].as<std::string>();
  }

  std::string table_prefix;
  if (vm.count("table_prefix")) {
    table_prefix = vm["table_prefix"].as<std::string>();
  }

  std::string table_name_override;
  if (vm.count("table_name_override")) {
    table_name_override = vm["table_name_override"].as<std::string>();
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


  bool postgres_disable_delete=false;
  if (vm.count("postgres_disable_delete")) {
    postgres_disable_delete = (vm["postgres_disable_delete"].as<int>() > 0);
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

  LOG(INFO) << "src_uri                      : " << src_uri;
  LOG(INFO) << "bb_api_key                   : " << bb_api_key;
  if (bb_secret_access_key.size()>0)
    LOG(INFO) << "bb_secret_access_key         : [hidden]";
  LOG(INFO) << "offset_storage               : " << offset_storage;
  LOG(INFO) << "topic                        : " << topic;
  LOG(INFO) << "start_offset                 : " << kspp::to_string(start_offset);
  LOG(INFO) << "postgres_host                : " << postgres_host;
  LOG(INFO) << "postgres_port                : " << postgres_port;
  LOG(INFO) << "postgres_dbname              : " << postgres_dbname;
  LOG(INFO) << "postgres_user                : " << postgres_user;
  LOG(INFO) << "postgres_password            : " << "[hidden]";
  LOG(INFO) << "postgres_max_items_in_insert : " << postgres_max_items_in_insert;
  LOG(INFO) << "id_column                    : " << id_column;
  LOG(INFO) << "postgres_warning_timeout     : " << postgres_warning_timeout;
  LOG(INFO) << "table_prefix                 : " << table_prefix;
  LOG(INFO) << "postgres_tablename           : " << postgres_tablename;
  LOG(INFO) << "character_encoding           : " << character_encoding;
  LOG(INFO) << "postgres_disable_delete      : " << postgres_disable_delete;
  LOG(INFO) << "pushgateway_uri              : " << pushgateway_uri;
  LOG(INFO) << "metrics_namespace            : " << metrics_namespace;

  kspp::connect::connection_params connection_params;
  connection_params.host = postgres_host;
  connection_params.port = postgres_port;
  connection_params.user = postgres_user;
  connection_params.password = postgres_password;
  connection_params.database_name = postgres_dbname;

  LOG(INFO) << "discovering facts...";
  if (oneshot)
    LOG(INFO) << "oneshot          : TRUE";

  kspp::topology_builder builder(config);
  grpc::ChannelArguments channelArgs;

  auto t = builder.create_topology();
  auto stream = t->create_processor<kspp::grpc_avro_source<kspp::generic_avro,kspp::generic_avro>>(0, topic, offset_storage, src_uri, bb_api_key, bb_secret_access_key);
  auto sink = t->create_sink<kspp::postgres_generic_avro_sink>(stream, postgres_tablename, connection_params, id_column, character_encoding, postgres_max_items_in_insert, postgres_disable_delete);

  std::map<std::string, std::string> labels = {
      { "app_name", SERVICE_NAME },
      //{ "app_realm", app_realm },
      { "hostname", default_hostname() },
      { "src_topic", topic },
      { "dst_uri", postgres_host },
      { "dst_database", postgres_dbname },
      { "dst_table", postgres_tablename }
  };

  t->add_labels(labels);
  t->start(start_offset);

  std::signal(SIGINT, sigterm);
  std::signal(SIGTERM, sigterm);
  std::signal(SIGPIPE, SIG_IGN);

  while (run) {
    auto sz = t->process(kspp::milliseconds_since_epoch());
    if (sz == 0) {
      std::this_thread::sleep_for(100ms);
      continue;
    }

    // commit offsets


    // send metrics

    }

  LOG(INFO) << "exiting";
  return 0;
}