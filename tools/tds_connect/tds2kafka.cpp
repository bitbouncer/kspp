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
#include <kspp/connect/avro_file/avro_file_sink.h>
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

static std::string get_env_with_default(const char *env, const char* default_value) {
  std::cerr << env << std::endl;
  const char *env_p = std::getenv(env);
  if (env_p)
    return std::string(env_p);
  return default_value;
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
      ("db_table", boost::program_options::value<std::string>()->default_value(get_env("DB_TABLE")),
       "db_table")
      ("db_polltime", boost::program_options::value<int32_t>()->default_value(60), "db_polltime")
      ("topic_prefix", boost::program_options::value<std::string>()->default_value(get_env_with_default("TOPIC_PREFIX", "DEV_sqlserver_")), "topic_prefix")
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

  int db_polltime;
  if (vm.count("db_polltime")) {
    db_polltime = vm["db_polltime"].as<int>();
  } else {
    std::cout << "--db_polltime must be specified" << std::endl;
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
  LOG(INFO) << "db_polltime           : " << db_polltime;
  LOG(INFO) << "topic_prefix          : " << topic_prefix;
  LOG(INFO) << "kafka_topic           : " << topic_name;

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

  setlocale(LC_ALL, "");

  kspp::topology_builder generic_builder("kspp", SERVICE_NAME + db_dbname, config);
  auto topology = generic_builder.create_topology();

  auto source0 = topology->create_processors<kspp::tds_generic_avro_source>({0}, db_table, connection_params, "id", "ts", config->get_schema_registry(),  std::chrono::seconds(db_polltime));


   if (filename.size()) {
    topology->create_sink<kspp::avro_file_sink>(source0, "/tmp/" + topic_prefix + db_table + ".avro");
  } else {
    topology->create_sink<kspp::kafka_sink<void, kspp::GenericAvro, void, kspp::avro_serdes>>(source0, topic_name, config->avro_serdes());
  }


  topology->init_metrics();
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
