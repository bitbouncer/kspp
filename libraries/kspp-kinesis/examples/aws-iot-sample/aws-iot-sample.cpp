#include <csignal>
#include <kspp/topology_builder.h>
#include <kspp/processors/flat_map.h>
#include <kspp/connect/aws/kinesis_source.h>
#include <kspp/connect/mqtt/mqtt_sink.h>
#include <nlohmann/json.hpp>

using namespace std::chrono_literals;
using namespace kspp;
using json = nlohmann::json;

static bool run = true;
static void sigterm(int sig) {
  run = false;
}

int main(int argc, char **argv) {
  if (argc!=2){
    std::cerr << "usage: " << argv[0] << " stream_name";
    return -1;
  }

  char *mqtt_endpoint = getenv("BB_MQTT_ENDPOINT");
  if (mqtt_endpoint == nullptr) {
    std::cerr << "BB_MQTT_ENDPOINT not defined - exiting" << std::endl;
    exit(-1);
  }

  std::string mqtt_username;
  std::string mqtt_password;

  std::string ssl_key_store;
  std::string ssl_private_key;

  auto u = getenv ("BB_MQTT_USERNAME");
  if (u)
    mqtt_username = u;

  auto p = getenv ("BB_MQTT_PASSWORD");
  if (p)
    mqtt_password = p;

  auto ks = getenv ("SSL_KEY_STORE");
  if (ks)
    ssl_key_store = ks;

  auto pk = getenv ("SSL_PRIVATE_KEY");
  if (pk)
    ssl_private_key = pk;

  LOG(INFO) << "BB_MQTT_ENDPOINT " << mqtt_endpoint;
  LOG(INFO) << "BB_MQTT_USERNAME " << mqtt_username;
  LOG(INFO) << "BB_MQTT_PASSWORD " << mqtt_password;
  LOG(INFO) << "SSL_KEY_STORE    " << ssl_key_store;
  LOG(INFO) << "SSL_PRIVATE_KEY  " << ssl_private_key;

  if (ssl_key_store.size() && !std::experimental::filesystem::exists(ssl_key_store)){
    std::cerr << "SSL_KEY_STORE at path: " << ssl_key_store << " not found - exiting";
    return -1;
  }

  if (ssl_private_key.size() && !std::experimental::filesystem::exists(ssl_private_key)){
    std::cerr << "SSL_PRIVATE_KEY at path: " << ssl_private_key << " not found - exiting";
    return -1;
  }

  mqtt::connect_options connOpts(mqtt_username, mqtt_password);
  connOpts.set_mqtt_version(MQTTVERSION_3_1_1);
  connOpts.set_keep_alive_interval(20);

  mqtt::ssl_options sslopts;
  sslopts.set_trust_store("/etc/ssl/certs/ca-certificates.crt");
  if (ssl_key_store.size() && ssl_private_key.size()){
    sslopts.set_key_store(ssl_key_store);
    sslopts.set_private_key(ssl_private_key);
  }
  connOpts.set_ssl(sslopts);


  FLAGS_logtostderr = 1;
  google::InitGoogleLogging(argv[0]);

  std::string stream_name = argv[1];

  /*auto nr_of_partitions = kspp::kafka::get_number_partitions(config, topic);
  if (partition_list.size() == 0 || partition_list[0] == -1)
    partition_list = kspp::get_partition_list(nr_of_partitions);
  LOG(INFO) << "partition_list   : " << kspp::partition_list_to_string(partition_list);
  */

  auto config = std::make_shared<kspp::cluster_config>("", 0);
  kspp::topology_builder generic_builder(config);

  auto t = generic_builder.create_topology();
  auto source0 = t->create_processors<kspp::kinesis_string_source>({0},stream_name);
  auto flatmap = t->create_processors<kspp::flat_map<std::string, std::string, std::string, std::string>>(source0, [](const auto record, auto sink){
    if (record.value()) {
      json j = json::parse(*record.value());
      double t0 = j["ts"];
      auto now = kspp::milliseconds_since_epoch();
      int64_t kinesis_lag = now - record.event_time();
      int64_t total_lag = now -t0;
      //LOG(INFO) << *record.value() << " kinesis ts: " << record.event_time() << ", kinesis lag: " << kinesis_lag << " total_lag: " << total_lag;
      //std::string msg = "{\"mob\": {\"lat\": 59.334591, \"lng\": 18.063240}, \"ts\":" + std::to_string(kspp::milliseconds_since_epoch()) + "}";
      insert(sink, std::string("alarm"), *record.value());
    }
    });
  auto sink = t->create_sink<kspp::mqtt_sink>(flatmap, mqtt_endpoint, connOpts);

  std::signal(SIGINT, sigterm);
  std::signal(SIGTERM, sigterm);
  std::signal(SIGPIPE, SIG_IGN);

  LOG(INFO) << "status is up";

  t->start(kspp::OFFSET_END); // only this is implemneted in kinesis

  while (run) {
    if (t->process(kspp::milliseconds_since_epoch()) == 0) {
      std::this_thread::sleep_for(5ms);
      t->commit(false);
    }
  }

  LOG(INFO) << "status is down";

  return 0;
}


