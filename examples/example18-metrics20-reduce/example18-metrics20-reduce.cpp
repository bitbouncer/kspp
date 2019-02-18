#include <iostream>
#include <chrono>
#include <kspp/topology_builder.h>
#include <kspp/sinks/kafka_sink.h>
#include <kspp/sources/kafka_source.h>
#include <kspp/processors/flat_map.h>
#include <kspp/utils/kafka_utils.h>
#include <kspp/avro/avro_serdes.h>
#include <kspp/impl/serdes/text_serdes.h>
#include <kspp/metrics/influx_metrics_reporter.h>
#include <kspp/utils/env.h>
#include <boost/lexical_cast.hpp>


#define SERVICE_NAME "aggregate-map"

using namespace std::chrono_literals;


std::string default_hostname() {
  char s[256];
  sprintf(s, "unknown");
  gethostname(s, 256);
  return std::string(s);
}

std::string default_metrics_tags(std::string app_name) {
  std::string s = "env=dev,serviceName=" + app_name + ",instanceId=dev,machineName=" + default_hostname();
  return s;
}

std::string to_string(const metrics20::avro::metrics20_key_t& key)
{
  std::string s;
  for (std::vector<metrics20::avro::metrics20_key_tags_t>::const_iterator i = key.tags.begin(); i!=key.tags.end(); ++i)
  {
    s += i->key + "=" + i->value;
    if (i != key.tags.end()-1){
      s += ", ";
    }
  }
  return s;
}


static bool run = true;
static void sigterm(int sig) {
  run = false;
}

int main(int argc, char **argv) {
  FLAGS_logtostderr = 1;
  google::InitGoogleLogging(argv[0]);

  std::string metrics_tags = default_metrics_tags(SERVICE_NAME);


  std::string consumer_group("kspp-examples");
  auto config = std::make_shared<kspp::cluster_config>(consumer_group);
  config->load_config_from_env();
  config->set_consumer_buffering_time(10ms);
  config->set_producer_buffering_time(10ms);
  config->validate();// optional
  config->log(); // optional

  auto partitions = kspp::kafka::get_number_partitions(config, "telegraf");
  auto partition_list = kspp::get_partition_list(partitions);
  kspp::topology_builder builder(config);
  auto topology = builder.create_topology();

  auto sources = topology->create_processors<kspp::kafka_source<metrics20::avro::metrics20_key_t, double, kspp::avro_serdes, kspp::avro_serdes>>(
      partition_list,
      "metrics2_telegraf_raw",
      config->avro_serdes(),
      config->avro_serdes());

  /*
  auto transforms = topology->create_processors<kspp::flat_map<std::string, std::string, metrics20::avro::metrics20_key_t, double>>(
      sources, [](const auto record, auto flat_map) {
        if (record->value()) {
          auto v = parse_line(record->value()->data(), record->value()->size());
          for (auto m : v) {
            flat_map->push_back(kspp::make_krecord(m.key, m.val, m.ts));
          }
        }
      });

  auto sink = topology->create_sink<kspp::kafka_sink<metrics20::avro::metrics20_key_t, double, kspp::avro_serdes, kspp::avro_serdes>>(
      "metrics2_telegraf_internal-mr",
      config->avro_serdes(),
      config->avro_serdes());

  for (auto i : transforms)
    i->add_sink(sink);
    */

  topology->start(kspp::OFFSET_STORED);
  //topology->start(kspp::OFFSET_BEGINNING);

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
    //auto metrics_reporter = std::make_shared<kspp::influx_metrics_reporter>(builder, "kspp_metrics", "kspp", metrics_tags) << topology;
    while (run) {
      if (topology->process(kspp::milliseconds_since_epoch()) == 0) {
        std::this_thread::sleep_for(10ms);
        topology->commit(false);
      }
    }
  }

  /*
  topology->for_each_metrics([](kspp::metric &m) {
    std::cerr << "metrics: " << m.name() << " " << m.tags() << " : " << m.value() << std::endl;
  });
  */

  topology->commit(true);
  topology->close();
  return 0;
}

