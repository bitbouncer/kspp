#include <iostream>
#include <csignal>
#include <boost/program_options.hpp>
#include <kspp/topology_builder.h>
#include <kspp/sources/avro_file_source.h>
#include <kspp/processors/flat_map.h>
#include <kspp/utils/env.h>
#include <kspp/connect/postgres/postgres_generic_avro_sink.h>
#include <kspp/processors/transform.h>
#include <kspp/utils/string_utils.h>
#include <nlohmann/json.hpp>
#include <sstream>

#define SERVICE_NAME "avro2pg"

using namespace std::chrono_literals;
using namespace kspp;
namespace fs = std::experimental::filesystem;
using json = nlohmann::json;


static bool run = true;

static void sigterm(int sig) {
  run = false;
}

void assign_record_members(const kspp::generic_avro::generic_record& src, kspp::generic_avro::generic_record& dst){
  for (auto i : dst.members()){
    //virtual size_t schema().names() const = 0;
    //virtual const std::string &nameAt(int index) const = 0;
    dst.set<std::string>(i, src.get<std::string>(i));
  }
}

int main(int argc, char **argv) {
  FLAGS_logtostderr = 1;
  google::InitGoogleLogging(argv[0]);

  boost::program_options::options_description desc("options");
  desc.add_options()
    ("help", "produce help message")
    ("src", boost::program_options::value<std::string>(), "src")
    ("postgres_host", boost::program_options::value<std::string>()->default_value(get_env_and_log("POSTGRES_HOST")), "postgres_host")
    ("postgres_port", boost::program_options::value<int32_t>()->default_value(5432), "postgres_port")
    ("postgres_user", boost::program_options::value<std::string>()->default_value(get_env_and_log("POSTGRES_USER")),"postgres_user")
    ("postgres_password", boost::program_options::value<std::string>()->default_value(get_env_and_log_hidden("POSTGRES_PASSWORD")),"postgres_password")
    ("postgres_dbname", boost::program_options::value<std::string>()->default_value(get_env_and_log("POSTGRES_DBNAME")),"postgres_dbname")
    ("postgres_table_name", boost::program_options::value<std::string>(), "postgres_table_name")
    ("postgres_max_items_in_insert", boost::program_options::value<int32_t>()->default_value(5000), "postgres_max_items_in_insert")
    ("postgres_warning_timeout", boost::program_options::value<int32_t>()->default_value(1000),"postgres_warning_timeout")
    ("keys", boost::program_options::value<std::string>(), "keys")
    ("character_encoding", boost::program_options::value<std::string>()->default_value("UTF8"), "character_encoding");

  boost::program_options::variables_map vm;
  boost::program_options::store(boost::program_options::parse_command_line(argc, argv, desc), vm);
  boost::program_options::notify(vm);

  if (vm.count("help")) {
    std::cout << desc << std::endl;
    return 0;
  }

  auto config = std::make_shared<kspp::cluster_config>("dummy", kspp::cluster_config::NONE);
  config->load_config_from_env();

  std::string src;
  if (vm.count("src")) {
    src = vm["src"].as<std::string>();
  } else {
    std::cout << "--src must be specified" << std::endl;
    return 0;
  }

  kspp::start_offset_t start_offset = kspp::OFFSET_BEGINNING;

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

  std::vector<std::string> keys;
  if (vm.count("keys")) {
    std::string s = vm["keys"].as<std::string>();
    keys = parse_string_array(s);
  }

  std::string postgres_table_name;
  if (vm.count("postgres_table_name")) {
    postgres_table_name = vm["postgres_table_name"].as<std::string>();
  } else {
    std::cout << "--postgres_table_name must be specified" << std::endl;
    return 0;
  }

  std::string character_encoding;
  if (vm.count("character_encoding")) {
    character_encoding = vm["character_encoding"].as<std::string>();
  } else {
    std::cout << "--character_encoding must be specified" << std::endl;
    return 0;
  }

  bool postgres_disable_delete = false;
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

  LOG(INFO) << "src                          : " << src;
  LOG(INFO) << "start_offset                 : " << kspp::to_string(start_offset);
  LOG(INFO) << "postgres_host                : " << postgres_host;
  LOG(INFO) << "postgres_port                : " << postgres_port;
  LOG(INFO) << "postgres_dbname              : " << postgres_dbname;
  LOG(INFO) << "postgres_user                : " << postgres_user;
  LOG(INFO) << "postgres_password            : " << "[hidden]";
  LOG(INFO) << "postgres_max_items_in_insert : " << postgres_max_items_in_insert;
  LOG(INFO) << "keys                         : " << kspp::to_string(keys);
  LOG(INFO) << "postgres_warning_timeout     : " << postgres_warning_timeout;
  LOG(INFO) << "postgres_table_name          : " << postgres_table_name;
  LOG(INFO) << "character_encoding           : " << character_encoding;

  kspp::connect::connection_params connection_params;
  connection_params.host = postgres_host;
  connection_params.port = postgres_port;
  connection_params.user = postgres_user;
  connection_params.password = postgres_password;
  connection_params.database_name = postgres_dbname;



  auto key_schema = std::make_shared<avro::ValidSchema>();
  {
    json j;
    j["type"] = "record";
    j["name"] = "pg_keys";
    j["fields"] = json::array();

    for (auto i : keys) {
      json column;
      column["name"] = i;
      column["type"] = "string";
      j["fields"].push_back(column);
    }
    std::cout << std::endl;

    std::stringstream s;
    s << j.dump(4) << std::endl;


    try {
      avro::compileJsonSchema(s, *key_schema);
    } catch (std::exception &e) {
      std::cerr << "exeption parsing schema " << e.what();
      return -1;
    }
  }

  kspp::generic_avro key_datum(key_schema, -1);

  LOG(INFO) << "discovering facts...";

  kspp::topology_builder builder(config);
  auto topology = builder.create_topology();
  auto source0 = topology->create_processors<kspp::generic_avro_file_source>({0}, src);
  auto transform = topology->create_processors<kspp::flat_map<void, kspp::generic_avro, kspp::generic_avro, kspp::generic_avro>>(
    source0, [&key_datum](const kspp::krecord<void, kspp::generic_avro>& in, auto self) {
      auto key = key_datum.mutable_record();
      assign_record_members(in.value()->record(), key);
      insert(self, key_datum, *in.value());
    });

  topology->create_sink<kspp::postgres_generic_avro_sink>(transform, postgres_table_name, connection_params, keys, character_encoding, postgres_max_items_in_insert, postgres_disable_delete);
  topology->start(start_offset);

  std::signal(SIGINT, sigterm);
  std::signal(SIGTERM, sigterm);
  std::signal(SIGPIPE, SIG_IGN);

  LOG(INFO) << "status is up";

  {
    int64_t next_exit_check = kspp::milliseconds_since_epoch() + 10000;
    while (run) {
      if (topology->process(kspp::milliseconds_since_epoch()) == 0) {
        std::this_thread::sleep_for(10ms);
        topology->commit(false);
      }

      if (kspp::milliseconds_since_epoch() > next_exit_check) {
        if (!topology->good()) {
          LOG(ERROR) << "NODES IN ERROR STATE - EXITING";
          run = false;
        }

        if (topology->eof()) {
          LOG(ERROR) << "EOF - EXITING";
          run = false;
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
