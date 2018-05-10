#include <iostream>
#include <csignal>
#include <boost/program_options.hpp>
#include <kspp/sinks/kafka_sink.h>
#include <kspp/metrics/influx_metrics_reporter.h>
#include <kspp/utils/env.h>
#include <boost/bind.hpp>
#include <boost/make_shared.hpp>
#include <kspp/connect/tds/tds_connection.h>
#include <kspp/connect/tds/tds_generic_avro_source.h>
#include <kspp/processors/transform.h>
#include <kspp/processors/flat_map.h>

#define SERVICE_NAME "tds2kafka"

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
      ("db_host", boost::program_options::value<std::string>()->default_value(get_env("DB_HOST")),
       "db_host")
      ("db_port", boost::program_options::value<int32_t>()->default_value(1433), "db_port")
      ("db_user", boost::program_options::value<std::string>()->default_value(get_env("DB_USER")),
       "db_user")
      ("db_password", boost::program_options::value<std::string>()->default_value(get_env("DB_PASSWORD")),
       "db_password")
      ("db_dbname", boost::program_options::value<std::string>()->default_value(get_env("DB_DBNAME")),
       "db_dbname")
      ("db_max_items_in_fetch", boost::program_options::value<int32_t>()->default_value(1000),
       "db_max_items_in_fetch")
      ("db_warning_timeout", boost::program_options::value<int32_t>()->default_value(1000),
       "db_warning_timeout")
      ("db_table", boost::program_options::value<std::string>()->default_value(get_env("DB_TABLE")),
       "db_table")
      ("topic_prefix", boost::program_options::value<std::string>()->default_value("postgres_"), "topic_prefix")
      ("filename", boost::program_options::value<std::string>(), "filename");

  boost::program_options::variables_map vm;
  boost::program_options::store(boost::program_options::parse_command_line(argc, argv, desc), vm);
  boost::program_options::notify(vm);

  if (vm.count("help")) {
    std::cout << desc << std::endl;
    return 0;
  }

  /*std::string metrics_topic;
  if (vm.count("metrics_topic")) {
    metrics_topic = vm["metrics_topic"].as<std::string>();
  } else {
    std::cout << "--metrics_topic must be specified" << std::endl;
    return 0;
  }
*/

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

  std::string db_host;
  if (vm.count("db_host")) {
    db_host = vm["db_host"].as<std::string>();
  } else {
    std::cout << "--db_host must be specified" << std::endl;
    return 0;
  }

  int db_port;
  if (vm.count("db_port")) {
    db_port = vm["db_port"].as<int>();
  } else {
    std::cout << "--db_port must be specified" << std::endl;
    return 0;
  }

  std::string db_dbname;
  if (vm.count("db_dbname")) {
    db_dbname = vm["db_dbname"].as<std::string>();
  } else {
    std::cout << "--db_dbname must be specified" << std::endl;
    return 0;
  }

  std::string db_table;
  if (vm.count("db_table")) {
    db_table = vm["db_table"].as<std::string>();
  } else {
    std::cout << "--db_table must be specified" << std::endl;
    return 0;
  }

  std::string db_user;
  if (vm.count("db_user")) {
    db_user = vm["db_user"].as<std::string>();
  } else {
    std::cout << "--db_user must be specified" << std::endl;
    return 0;
  }

  std::string db_password;
  if (vm.count("db_password")) {
    db_password = vm["db_password"].as<std::string>();
  } else {
    std::cout << "--db_password must be specified" << std::endl;
    return 0;
  }

  int db_max_items_in_fetch;
  if (vm.count("db_max_items_in_fetch")) {
    db_max_items_in_fetch = vm["db_max_items_in_fetch"].as<int>();
  } else {
    std::cout << "--db_max_items_in_fetch must be specified" << std::endl;
    return 0;
  }

  int db_warning_timeout;
  if (vm.count("db_warning_timeout")) {
    db_warning_timeout = vm["db_warning_timeout"].as<int>();
  } else {
    std::cout << "--db_warning_timeout must be specified" << std::endl;
    return 0;
  }

  std::string topic_prefix;
  if (vm.count("topic_prefix")) {
    topic_prefix = vm["topic_prefix"].as<std::string>();
  } else {
    std::cout << "--topic_prefix must be specified" << std::endl;
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

  std::string topic_name = topic_prefix + db_table;

  LOG(INFO) << "db_host               : " << db_host;
  LOG(INFO) << "db_port               : " << db_port;
  LOG(INFO) << "db_dbname             : " << db_dbname;
  LOG(INFO) << "db_table              : " << db_table;
  LOG(INFO) << "db_user               : " << db_user;
  LOG(INFO) << "db_password           : " << "[hidden]";
  LOG(INFO) << "db_max_items_in_fetch : " << db_max_items_in_fetch;
  LOG(INFO) << "db_warning_timeout    : " << db_warning_timeout;
  LOG(INFO) << "topic_prefix                : " << topic_prefix;
  LOG(INFO) << "kafka_topic                 : " << topic_name;


  std::string connect_string =
      "host=" + db_host + " port=" + std::to_string(db_port) + " user=" + db_user + " password=" +
          db_password + " dbname=" + db_dbname;

  if (filename.size()) {
     LOG(INFO) << "using avro file..";
    LOG(INFO) << "filename                   : " << filename;
  }

  LOG(INFO) << "discovering facts...";

  setlocale(LC_ALL, "");

  kspp::topology_builder generic_builder("kspp", SERVICE_NAME, config);
  auto topology = generic_builder.create_topology();

  auto source0 = topology->create_processors<kspp::tds_generic_avro_source>({0}, db_table, db_host, db_user, db_password, db_dbname, "id", "", config->get_schema_registry());

   /*
    * if (filename.size()) {
    topology->create_sink<kspp::avro_file_sink>(source0, "/tmp/" + topic_prefix + db_table + ".avro");
  } else {
    topology->create_sink<kspp::kafka_sink<void, kspp::GenericAvro, void, kspp::avro_serdes>>(source0, topic_name, config->avro_serdes());
  }
    */



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
