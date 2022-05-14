#include <csignal>
#include <kspp/topology_builder.h>
#include <kspp/processors/visitor.h>
#include <kspp/sources/mem_stream_source.h>
#include <nlohmann/json.hpp>

#include <kspp-mqtt/mqtt_sink.h>

using namespace std;
using namespace std::chrono;
using namespace std::chrono_literals;
using namespace kspp;
using json = nlohmann::json;

static bool run = true;

static void sigterm(int sig) {
  run = false;
}

const auto PERIOD = seconds(5);
const int MAX_BUFFERED_MSGS = 120;  // 120 * 5sec => 10min off-line buffering


int main(int argc, char **argv) {
  char *mqtt_endpoint = getenv("BB_MQTT_ENDPOINT");
  if (mqtt_endpoint == nullptr) {
    std::cerr << "BB_MQTT_ENDPOINT not defined - exiting" << std::endl;
    exit(-1);
  }

  std::string mqtt_username;
  std::string mqtt_password;

  std::string ssl_key_store;
  std::string ssl_private_key;

  auto u = getenv("BB_MQTT_USERNAME");
  if (u)
    mqtt_username = u;

  auto p = getenv("BB_MQTT_PASSWORD");
  if (p)
    mqtt_password = p;

  auto ks = getenv("SSL_KEY_STORE");
  if (ks)
    ssl_key_store = ks;

  auto pk = getenv("SSL_PRIVATE_KEY");
  if (pk)
    ssl_private_key = pk;


  LOG(INFO) << "BB_MQTT_ENDPOINT " << mqtt_endpoint;
  LOG(INFO) << "BB_MQTT_USERNAME " << mqtt_username;
  LOG(INFO) << "BB_MQTT_PASSWORD " << mqtt_password;
  LOG(INFO) << "SSL_KEY_STORE    " << ssl_key_store;
  LOG(INFO) << "SSL_PRIVATE_KEY  " << ssl_private_key;

  mqtt::connect_options connOpts(mqtt_username, mqtt_password);
  connOpts.set_mqtt_version(MQTTVERSION_3_1_1);
  connOpts.set_keep_alive_interval(MAX_BUFFERED_MSGS * PERIOD);

  mqtt::ssl_options sslopts;
  sslopts.set_trust_store("/etc/ssl/certs/ca-certificates.crt");
  if (ssl_key_store.size() && ssl_private_key.size()) {
    sslopts.set_key_store(ssl_key_store);
    sslopts.set_private_key(ssl_private_key);
  }
  connOpts.set_ssl(sslopts);

  FLAGS_logtostderr = 1;
  google::InitGoogleLogging(argv[0]);

  auto config = std::make_shared<kspp::cluster_config>("", 0);
  kspp::topology_builder builder(config);
  auto topology = builder.create_topology();
  auto source = topology->create_processor<kspp::mem_stream_source<std::string, std::string>>(0);
  auto sink = topology->create_sink<kspp::mqtt_sink>(source, mqtt_endpoint, connOpts);
  topology->start(kspp::OFFSET_BEGINNING); // has to be something - since we feed events from mem - totally irrelevant

  std::thread t([topology]() {
    while (run) {
      if (topology->process(kspp::milliseconds_since_epoch()) == 0) {
        std::this_thread::sleep_for(5ms);
        topology->commit(false);
      }
    }
    LOG(INFO) << "flushing events..";
    topology->flush(true, 10000); // 10sec max
    LOG(INFO) << "flushing events done";
  });

  for (int i = 0; i != 10; ++i) {
    for (int j = 0; j != 10; ++j) {
      std::string msg = "{\"mob\": {\"lat\": 59.334591, \"lng\": 18.063240}, \"ts\":" +
                        std::to_string(kspp::milliseconds_since_epoch()) + "}";
      insert(*source, std::string("alarm"), msg);
    }
    std::this_thread::sleep_for(1000ms);
  }
  LOG(INFO) << "exiting test";
  run = false;
  t.join();
  LOG(INFO) << "status is down";
  return 0;
}


