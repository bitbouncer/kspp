#include <iostream>
#include <csignal>
#include <boost/program_options.hpp>
#include <kspp/processors/filter.h>
#include <kspp/sources/kafka_source.h>
#include <kspp/sinks/kafka_sink.h>
#include <kspp/metrics/influx_metrics_reporter.h>
#include <kspp/utils/env.h>


#include <kspp/impl/connect/postgres/postgres_asio.h>
#include <kspp/impl/connect/postgres/avro_postgres.h>

#include <boost/bind.hpp>
#include <boost/make_shared.hpp>
#include <avro/Generic.hh>
#include <avro/DataFile.hh>

#define SERVICE_NAME "postgres2kafka"

using namespace std::chrono_literals;

static bool run = true;
static void sigterm(int sig) {
  run = false;
}

static std::string get_env(const char *env) {
  const char *env_p = std::getenv(env);
  if (env_p) {
    return std::string(env_p);
  return "";
  }
}

void
handle_fetch100(std::shared_ptr<postgres_asio::connection> connection, boost::shared_ptr<avro::ValidSchema> schema,
                size_t total_count, size_t max_items_in_fetch, int ec, std::shared_ptr<PGresult> res,
                boost::shared_ptr<avro::DataFileWriter<avro::GenericDatum>> file_writer) {
  if (ec)
    return;

  int tuples_in_batch = PQntuples(res.get());
  total_count += tuples_in_batch;
  //std::cerr << "got " << tuples_in_batch << ", total: " << total_count << std::endl;
  if (tuples_in_batch == 0) {
    LOG(INFO) << "querty done, got total: " << total_count;
    connection->exec("CLOSE mycursor; COMMIT", [connection](int ec, std::shared_ptr<PGresult> res) {});
    file_writer->flush();
    file_writer->close();
    return;
  } else {
    auto v = to_avro2(schema, res);

    for (auto i = v.begin(); i != v.end(); ++i)
      file_writer->write(**i);

    /*
    auto stream = avro::ostreamOutputStream(std::cerr);
    auto encoder = avro::jsonEncoder(*schema);
    encoder->init(*stream);

    avro::GenericWriter writer(*schema, encoder);
    writer.write(*gd);
    encoder->flush();
    stream->flush();
    */

    // just let them die for now....
  }
  //std::cerr << ".";
  connection->exec("FETCH " + std::to_string(max_items_in_fetch) +" in mycursor", [connection, schema, total_count, max_items_in_fetch, file_writer](int ec, std::shared_ptr<PGresult> res) {
    handle_fetch100(connection, schema, total_count, max_items_in_fetch, ec, std::move(res), file_writer);
  });
}


int main(int argc, char** argv) {
  FLAGS_logtostderr = 1;
  google::InitGoogleLogging(argv[0]);

  boost::program_options::options_description desc("options");
  desc.add_options()
      ("help", "produce help message")
      ("broker", boost::program_options::value<std::string>()->default_value(kspp::default_kafka_broker_uri()), "broker")
      ("schema_registry", boost::program_options::value<std::string>()->default_value(kspp::default_schema_registry_uri()), "schema_registry")
      ("postgres_host", boost::program_options::value<std::string>()->default_value(get_env("POSTGRES_HOST")), "postgres_host")
      ("postgres_port", boost::program_options::value<int32_t>()->default_value(5432), "postgres_port")
      ("postgres_user", boost::program_options::value<std::string>()->default_value(get_env("POSTGRES_USER")), "postgres_user")
      ("postgres_password", boost::program_options::value<std::string>()->default_value(get_env("POSTGRES_PASSWORD")), "postgres_password")
      ("postgres_max_items_in_fetch", boost::program_options::value<int32_t>()->default_value(1000), "postgres_max_items_in_fetch")
      ("postgres_warning_timeout", boost::program_options::value<int32_t>()->default_value(1000), "postgres_warning_timeout")
      ("table", boost::program_options::value<std::string>()->default_value(get_env("POSTGRES_TABLE")), "table")
      ("topic_prefix", boost::program_options::value<std::string>()->default_value("postgres_"), "topic_prefix")
      ;

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

  auto config = std::make_shared<kspp::cluster_config>();
  config->set_brokers(broker);
  config->set_schema_registry(schema_registry);
  config->set_producer_buffering_time(1000ms);
  config->set_consumer_buffering_time(500ms);
  config->set_ca_cert_path(kspp::default_ca_cert_path());
  config->set_private_key_path(kspp::default_client_cert_path(),
                               kspp::default_client_key_path(),
                               kspp::default_client_key_passphrase());
  config->validate();
  config->log();

  LOG(INFO) << "postgres_host               : " << postgres_host;
  LOG(INFO) << "postgres_port               : " << postgres_port;
  LOG(INFO) << "postgres_dbname             : " << postgres_dbname;
  LOG(INFO) << "postgres_table              : " << postgres_table;
  LOG(INFO) << "postgres_user               : " << postgres_user;
  LOG(INFO) << "postgres_password           : " << "[hidden]";
  LOG(INFO) << "postgres_max_items_in_fetch : " << postgres_max_items_in_fetch;
  LOG(INFO) << "postgres_warning_timeout    : " << postgres_warning_timeout;
  LOG(INFO) << "topic_prefix                : " << topic_prefix;

  std::string connect_string = "host=" + postgres_host + " port=" + std::to_string(postgres_port) + " user=" +  postgres_user + " password=" + postgres_password + " dbname=" + postgres_dbname;

  LOG(INFO) << "discovering facts...";

  //auto avro_schema_registry = std::make_shared<kspp::avro_schema_registry>(config);
  //auto avro_serdes = std::make_shared<kspp::avro_serdes>(avro_schema_registry);

  kspp::topology_builder generic_builder("kspp", SERVICE_NAME, config);
  auto topology = generic_builder.create_topology();

  signal(SIGINT, sigterm);
  signal(SIGTERM, sigterm);


  boost::asio::io_service fg_ios;
  boost::asio::io_service bg_ios;
  std::auto_ptr<boost::asio::io_service::work> work2(new boost::asio::io_service::work(fg_ios));
  std::auto_ptr<boost::asio::io_service::work> work1(new boost::asio::io_service::work(bg_ios));
  std::thread fg(boost::bind(&boost::asio::io_service::run, &fg_ios));
  std::thread bg(boost::bind(&boost::asio::io_service::run, &bg_ios));

  auto connection = std::make_shared<postgres_asio::connection>(fg_ios, bg_ios);
  connection->set_warning_timout(postgres_warning_timeout);
  connection->connect(connect_string, [connection, postgres_table, postgres_max_items_in_fetch, topic_prefix](int ec) {
    if (!ec) {
      bool r1 = connection->set_client_encoding("UTF8");
      connection->exec("BEGIN", [connection, postgres_table, postgres_max_items_in_fetch, topic_prefix](int ec, std::shared_ptr<PGresult> res) {
        if (ec) {
          LOG(FATAL) << "BEGIN failed ec:" << ec << " last_error: " << connection->last_error();
          return;
        }
        std::string fields = "*";
        connection->exec("DECLARE mycursor CURSOR FOR SELECT " + fields + " FROM "+ postgres_table,
                         [connection, postgres_max_items_in_fetch, topic_prefix, postgres_table](int ec, std::shared_ptr<PGresult> res) {
                           if (ec) {
                             LOG(FATAL) << "DECLARE mycursor... failed ec:" << ec << " last_error: " << connection->last_error();
                             return;
                           }
                           connection->exec("FETCH " + std::to_string(postgres_max_items_in_fetch) +" in mycursor",
                                            [connection, postgres_max_items_in_fetch, topic_prefix, postgres_table](int ec, std::shared_ptr<PGresult> res) {
                                              try {
                                                std::string schema_name_base = topic_prefix + postgres_table;

                                                boost::shared_ptr<avro::ValidSchema> schema(
                                                    valid_schema_for_table_row(schema_name_base  + ".value", res));
                                                boost::shared_ptr<avro::ValidSchema> key_schema(
                                                    valid_schema_for_table_key(schema_name_base + ".key", {"id"}, res));

                                                std::cerr << "key schema" << std::endl;
                                                key_schema->toJson(std::cerr);
                                                std::cerr << std::endl;

                                                std::cerr << "value schema" << std::endl;
                                                std::cerr << std::endl;
                                                schema->toJson(std::cerr);
                                                std::cerr << std::endl;

                                                boost::filesystem::path path(".");
                                                bool exists1 = boost::filesystem::exists(path);
                                                path /= "test1.avro";
                                                bool exists2 = boost::filesystem::exists(path);

                                                //avro::DataFileWriter<avro::GenericDatum> fw(filename.c_str(), *schema, 16 * 1024, avro::NULL_CODEC);
                                                boost::shared_ptr<avro::DataFileWriter<avro::GenericDatum>> file_writer = boost::make_shared<avro::DataFileWriter<avro::GenericDatum>>(
                                                    path.generic_string().c_str(), *schema, 16 * 1024,
                                                    avro::DEFLATE_CODEC); //AVRO_DEFLATE_CODEC, NULL_CODEC

                                                handle_fetch100(connection, schema, 0, postgres_max_items_in_fetch, ec, std::move(res), file_writer);
                                              }
                                              catch (std::exception &e) {
                                                std::cerr << "exception: " << e.what() << std::endl;
                                              };
                                              /*
                                              int nFields = PQnfields(res.get());
                                              for (int i = 0; i < nFields; i++)
                                                  printf("%-15s", PQfname(res.get(), i));
                                              */


                                            });
                         });
      });
    }
  });

  while (run) {
    std::this_thread::sleep_for(1s);
  }

  work1.reset();
  work2.reset();
  //bg_ios.stop();
  //fg_ios.stop();
  bg.join();
  fg.join();




  //auto sources = topology->create_processors<kspp::kafka_source<void, std::string, kspp::text_serdes>>(partition_list, metrics_topic);
  //auto sink   = topology->create_sink<influx_sink>(sources, "base url", http_batch_size, http_timeout);

  //topology->init_metrics();
  //topology->start(kspp::OFFSET_STORED);
  //topology->init();

  // output metrics and run...
  /*{
    //auto metrics_reporter = std::make_shared<kspp::influx_metrics_reporter>(generic_builder, metrics_topic, "kspp", "") << topology;
    while (run) {
      if (topology->process_one() == 0) {
        std::this_thread::sleep_for(10ms);
        topology->commit(false);
      }
    }
  }
  topology->commit(true);
  topology->close();
   */
  return 0;
}
