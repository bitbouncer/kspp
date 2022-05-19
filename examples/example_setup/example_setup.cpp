#include <iostream>
#include <chrono>
#include <boost/uuid/uuid.hpp>
#include <boost/functional/hash.hpp>
#include <kspp/serdes/binary_serdes.h>
#include <kspp/topology_builder.h>
#include <kspp/sinks/kafka_sink.h>
#include <kspp/utils/env.h>

using namespace std::chrono_literals;

static boost::uuids::uuid to_uuid(int64_t x) {
  boost::uuids::uuid uuid;
  memset(uuid.data, 0, 16);
  memcpy(uuid.data, &x, 8);
  return uuid;
}

int main(int argc, char **argv) {
  FLAGS_logtostderr = 1;
  google::InitGoogleLogging(argv[0]);

  std::string consumer_group("kspp-examples");
  auto config = std::make_shared<kspp::cluster_config>(consumer_group);
  config->load_config_from_env();
  config->validate(); // optional
  config->log();// optional

  kspp::topology_builder builder(config);
  auto topology = builder.create_topology();
  auto table_stream = topology->create_sink<kspp::kafka_sink<boost::uuids::uuid, int64_t, kspp::binary_serdes, kspp::binary_serdes>>(
      "kspp_test0_table");
  auto event_stream = topology->create_sink<kspp::kafka_sink<boost::uuids::uuid, int64_t, kspp::binary_serdes, kspp::binary_serdes>>(
      "kspp_test0_eventstream");

  std::vector<boost::uuids::uuid> ids;
  for (int i = 0; i != 10000; ++i)
    ids.push_back(to_uuid(i));

  std::cerr << "creating " << table_stream->log_name() << std::endl;
  for (int64_t update_nr = 0; update_nr != 100; ++update_nr) {
    for (auto &i: ids) {
      table_stream->push_back(i, update_nr);
    }
  }

  std::cerr << "creating " << event_stream->log_name() << std::endl;
  for (int64_t event_nr = 0; event_nr != 100; ++event_nr) {
    for (auto &i: ids) {
      event_stream->push_back(i, event_nr);
    }
  }

  topology->flush();

  return 0;
}
