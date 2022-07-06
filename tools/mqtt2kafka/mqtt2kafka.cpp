#include "mqtt/async_client.h"
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <csignal>
#include <glog/logging.h>
#include <iostream>
#include <string>
#include <thread>
#include <librdkafka/rdkafkacpp.h>

using namespace std;
using namespace chrono_literals;

// const string SERVER_ADDRESS	{ "tcp://localhost:1883" };
const string CLIENT_ID{"paho_cpp_async_consume"};
//const string MQTT_TOPIC{"#"};

const int QOS = 1;

/////////////////////////////////////////////////////////////////////////////

static volatile sig_atomic_t run = 1;

static void sigterm(int sig) {
  run = 0;
}

static void set_config(RdKafka::Conf *conf, std::string key, std::string value) {
  std::string errstr;
  if (conf->set(key, value, errstr) != RdKafka::Conf::CONF_OK) {
    throw std::invalid_argument("\"" + key + "\" -> " + value + ", error: " + errstr);
  }
  DLOG(INFO) << "rd_kafka set_config: " << key << "->" << value;
}

static void set_config(RdKafka::Conf *conf, std::string key, RdKafka::DeliveryReportCb *callback) {
  std::string errstr;
  if (conf->set(key, callback, errstr) != RdKafka::Conf::CONF_OK) {
    throw std::invalid_argument("\"" + key + "\", error: " + errstr);
  }
}

class ExampleDeliveryReportCb : public RdKafka::DeliveryReportCb {
public:
  void dr_cb(RdKafka::Message &message) {
    /* If message.err() is non-zero the message delivery failed permanently
     * for the message. */
    if (message.err())
      LOG(ERROR) << "% Message delivery failed: " << message.errstr();
    else
      ;
      //LOG(INFO) << "% Message delivered to topic " << message.topic_name()
      //          << " [" << message.partition() << "] at offset "
      //          << message.offset();
  }
};



int main(int argc, char *argv[]) {
  char *mqtt_endpoint = getenv("MQTT_ENDPOINT");
  if (mqtt_endpoint == nullptr) {
    std::cerr << "MQTT_ENDPOINT not defined - exiting" << std::endl;
    exit(-1);
  }

  auto mqtt_topic = getenv("MQTT_TOPIC");
  if (mqtt_topic == nullptr) {
    std::cerr << "MQTT_TOPIC not defined - exiting" << std::endl;
    exit(-1);
  }

  auto kafka_broker = getenv("KAFKA_BROKER");
  if (kafka_broker == nullptr) {
    std::cerr << "KAFKA_BROKER not defined - exiting" << std::endl;
    exit(-1);
  }

  auto topic = getenv("KAFKA_TOPIC");
  if (topic == nullptr) {
    std::cerr << "KAFKA_TOPIC not defined - exiting" << std::endl;
    exit(-1);
  }

  std::string mqtt_username;
  std::string mqtt_password;

  std::string ssl_key_store;
  std::string ssl_private_key;

  auto u = getenv("MQTT_USERNAME");
  if (u)
    mqtt_username = u;

  auto p = getenv("MQTT_PASSWORD");
  if (p)
    mqtt_password = p;

  auto ks = getenv("SSL_KEY_STORE");
  if (ks)
    ssl_key_store = ks;

  auto pk = getenv("SSL_PRIVATE_KEY");
  if (pk)
    ssl_private_key = pk;



  LOG(INFO) << "MQTT_ENDPOINT     " << mqtt_endpoint;
  LOG(INFO) << "MQTT_TOPIC        " << mqtt_topic;
  LOG(INFO) << "MQTT_USERNAME     " << mqtt_username;
  LOG(INFO) << "MQTT_PASSWORD     " << mqtt_password;
  LOG(INFO) << "SSL_KEY_STORE     " << ssl_key_store;
  LOG(INFO) << "SSL_PRIVATE_KEY   " << ssl_private_key;
  LOG(INFO) << "KAFKA_BROKER      " << kafka_broker;
  LOG(INFO) << "KAFKA_TOPIC       " << topic;


  mqtt::connect_options connOpts(mqtt_username, mqtt_password);
  connOpts.set_mqtt_version(MQTTVERSION_3_1_1);
  connOpts.set_keep_alive_interval(10);

  mqtt::ssl_options sslopts;
  /*
  sslopts.set_trust_store("/etc/ssl/certs/ca-certificates.crt");
  if (ssl_key_store.size() && ssl_private_key.size()) {
    sslopts.set_key_store(ssl_key_store);
    sslopts.set_private_key(ssl_private_key);
  }
  connOpts.set_ssl(sslopts);
  */
  connOpts.set_clean_session(true); // this should maybe false -> we keep
  // session bu we also need to empty broker

  mqtt::async_client cli(mqtt_endpoint, CLIENT_ID);
  //	auto connOpts = mqtt::connect_options_builder()
  //		.clean_session(false)
  //		.finalize();

  RdKafka::Conf *conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
  set_config(conf, "bootstrap.servers", kafka_broker);
  set_config(conf, "queue.buffering.max.ms", "15");
  set_config(conf, "message.timeout.ms", "1000000");
  set_config(conf, "socket.nagle.disable", "true");
  set_config(conf, "socket.max.fails", "1000000");
  set_config(conf, "message.send.max.retries", "1000000");
  set_config(conf, "log.connection.close", "false");

  ExampleDeliveryReportCb ex_dr_cb;
  set_config(conf, "dr_cb", &ex_dr_cb);

  /*
   * Create producer instance.
   */
  std::string errstr;
  RdKafka::Producer *producer = RdKafka::Producer::create(conf, errstr);
  if (!producer) {
    std::cerr << "Failed to create producer: " << errstr << std::endl;
    exit(1);
  }
  delete conf;


  try {
    // Start consumer before connecting to make sure to not miss messages

    cli.start_consuming();

    // Connect to the server

    LOG(INFO) << "Connecting to the MQTT server...";
    auto tok = cli.connect(connOpts);

    // Getting the connect response will block waiting for the
    // connection to complete.
    auto rsp = tok->get_connect_response();

    // If there is no session present, then we need to subscribe, but if
    // there is a session, then the server remembers us and our
    // subscriptions.
    if (!rsp.is_session_present())
      cli.subscribe(mqtt_topic, QOS)->wait();

    // Consume messages
    // This just exits if the client is disconnected.
    // (See some other examples for auto or manual reconnect)

    LOG(INFO) << "Waiting for messages on topic: '" << mqtt_topic;

    signal(SIGINT, sigterm);
    signal(SIGTERM, sigterm);

    //mqtt::const_message_ptr *msg = new mqtt::const_message_ptr;
    while (run) {
      std::shared_ptr<const mqtt::message> msg;
      producer->poll(0);
      if (!cli.try_consume_message_for(&msg, 100ms))
        continue;
      //auto msg = cli.consume_message();
      if (!msg)
        break;
      LOG(INFO) << msg->get_topic() << ": " << msg->to_string();

      retry:
      RdKafka::ErrorCode err = producer->produce(
          /* Topic name */
          topic,
          RdKafka::Topic::PARTITION_UA,
          RdKafka::Producer::RK_MSG_COPY /* Copy payload */,
          /* Value */
          const_cast<char *>(msg->get_payload_str().c_str()), msg->get_payload_str().size(),
          /* Key */
          const_cast<char *>(msg->get_topic().c_str()), msg->get_topic().size(),
          /* Timestamp (defaults to current time) */
          0,
          /* Message headers, if any */
          NULL,
          /* Per-message opaque value passed to
           * delivery report */
          NULL);

      if (err != RdKafka::ERR_NO_ERROR) {
        LOG(ERROR) << "% Failed to produce to topic " << topic << ": "
                  << RdKafka::err2str(err);

        if (err == RdKafka::ERR__QUEUE_FULL) {
          producer->poll(1000 /*block for max 1000ms*/);
          goto retry;
        }
      }
      msg.reset();
    } // while run

    // If we're here, the client was almost certainly disconnected.
    // But we check, just to make sure.

    if (cli.is_connected()) {
      LOG(INFO) << "Shutting down and disconnecting from the MQTT server...";
      cli.unsubscribe(mqtt_topic)->wait();
      cli.stop_consuming();
      cli.disconnect()->wait();
      LOG(INFO) << "OK";
    } else {
      LOG(INFO) << "\nClient was disconnected";
    }
  } catch (const mqtt::exception &exc) {
    LOG(ERROR) << exc;
    return 1;
  }

  /* Wait for final messages to be delivered or fail.
   * flush() is an abstraction over poll() which
   * waits for all messages to be delivered. */
  LOG(INFO) << "Flushing final messages...";
  producer->flush(10 * 1000 /* wait for max 10 seconds */);

  if (producer->outq_len() > 0)
    LOG(INFO) << producer->outq_len() << " message(s) were not delivered";

  delete producer;
  return 0;
}
