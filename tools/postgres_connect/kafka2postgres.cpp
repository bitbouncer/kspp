#include <iostream>
#include <csignal>
#include <boost/program_options.hpp>
#include <kspp/topology_builder.h>
#include <kspp/sources/kafka_source.h>
#include <kspp/utils/env.h>
#include <kspp/utils/kafka_utils.h>
#include <kspp/connect/postgres/postgres_generic_avro_sink.h>
#include <kspp/processors/transform.h>
#include <kspp/metrics/prometheus_pushgateway_reporter.h>
#include <kspp/utils/string_utils.h>

#define SERVICE_NAME "kafka2postgres"

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
      ("topic", boost::program_options::value<std::string>(), "topic")
      ("partition_list", boost::program_options::value<std::string>()->default_value("[-1]"), "partition_list")
      ("start_offset", boost::program_options::value<std::string>()->default_value("OFFSET_BEGINNING"), "start_offset")
      ("postgres_host", boost::program_options::value<std::string>()->default_value(get_env_and_log("POSTGRES_HOST")), "postgres_host")
      ("postgres_port", boost::program_options::value<int32_t>()->default_value(5432), "postgres_port")
      ("postgres_user", boost::program_options::value<std::string>()->default_value(get_env_and_log("POSTGRES_USER")), "postgres_user")
      ("postgres_password", boost::program_options::value<std::string>()->default_value(get_env_and_log_hidden("POSTGRES_PASSWORD")), "postgres_password")
      ("postgres_dbname", boost::program_options::value<std::string>()->default_value(get_env_and_log("POSTGRES_DBNAME")), "postgres_dbname")
      ("postgres_max_items_in_insert", boost::program_options::value<int32_t>()->default_value(1000), "postgres_max_items_in_insert")
      ("postgres_warning_timeout", boost::program_options::value<int32_t>()->default_value(1000), "postgres_warning_timeout")
      ("postgres_disable_delete", boost::program_options::value<int32_t>(), "postgres_disable_delete")
      ("id_column", boost::program_options::value<std::string>()->default_value("id"), "id_column")
      ("table_prefix", boost::program_options::value<std::string>()->default_value("kafka_"), "table_prefix")
      ("character_encoding", boost::program_options::value<std::string>()->default_value("UTF8"), "character_encoding")
      ("table_name_override", boost::program_options::value<std::string>(), "table_name_override")
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

  bool postgres_disable_delete=false;
  if (vm.count("postgres_disable_delete")) {
    postgres_disable_delete = (vm["postgres_disable_delete"].as<int>() > 0);
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

  std::string table_name = table_prefix + topic;

  if (table_name_override.size())
    table_name=table_name_override;

  LOG(INFO) << "app_realm                    : " << app_realm;
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
  LOG(INFO) << "table_name_override          : " << table_name_override;
  LOG(INFO) << "table_name                   : " << table_name;
  LOG(INFO) << "character_encoding           : " << character_encoding;
  LOG(INFO) << "postgres_disable_delete      : " << postgres_disable_delete;
  LOG(INFO) << "pushgateway_uri              : " << config->get_pushgateway_uri();
  LOG(INFO) << "metrics_namespace            : " << metrics_namespace;

  kspp::connect::connection_params connection_params;
  connection_params.host = postgres_host;
  connection_params.port = postgres_port;
  connection_params.user = postgres_user;
  connection_params.password = postgres_password;
  connection_params.database_name = postgres_dbname;

  LOG(INFO) << "discovering facts...";

  auto nr_of_partitions = kspp::kafka::get_number_partitions(config, topic);
  if (partition_list.size() == 0 || partition_list[0] == -1)
    partition_list = kspp::get_partition_list(nr_of_partitions);
  LOG(INFO) << "partition_list   : " << kspp::partition_list_to_string(partition_list);

  kspp::topology_builder builder(config);
  auto topology = builder.create_topology();

  auto source0 = topology->create_processors<kspp::kafka_source<kspp::generic_avro, kspp::generic_avro, kspp::avro_serdes, kspp::avro_serdes>>(partition_list, topic, config->avro_serdes(), config->avro_serdes());
  /*https://www.postgresql.org/docs/9.3/static/multibyte.html*/
  /*auto transform = topology->create_processors<kspp::flat_map<kspp::generic_avro, kspp::generic_avro, kspp::generic_avro, kspp::generic_avro>>(
      source0, [](const kspp::krecord<kspp::generic_avro, kspp::generic_avro>& in, auto self) {
        insert(self, in);
      });
  */
  topology->create_sink<kspp::postgres_generic_avro_sink>(source0, table_name, connection_params, id_column, character_encoding, postgres_max_items_in_insert, postgres_disable_delete);

  std::string hostname = default_hostname();

  topology->add_labels( {
                            { "app_name", SERVICE_NAME },
                            { "app_realm", app_realm },
                            { "hostname", hostname },
                            { "db_host", postgres_host },
                            { "dst_table", table_name }
                        });

  topology->start(start_offset);

  std::signal(SIGINT, sigterm);
  std::signal(SIGTERM, sigterm);
  std::signal(SIGPIPE, SIG_IGN);

  LOG(INFO) << "status is up";

  {
    auto metrics_reporter = std::make_shared<kspp::prometheus_pushgateway_reporter>(metrics_namespace, config->get_pushgateway_uri()) << topology;
    int64_t next_exit_check = kspp::milliseconds_since_epoch() + 10000;
    while (run) {
      if (topology->process(kspp::milliseconds_since_epoch()) == 0) {
        std::this_thread::sleep_for(10ms);
        topology->commit(false);
      }

      if (kspp::milliseconds_since_epoch()>next_exit_check){
        if (!topology->good()){
          LOG(ERROR) << "NODES IN ERROR STATE - EXITING";
          run=false;
        }
        next_exit_check = kspp::milliseconds_since_epoch() + 10000;
      }
    }
  }

  topology->commit(true);
  topology->close();
  LOG(INFO) << "status is down";
  return 0;
}
