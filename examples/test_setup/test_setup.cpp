#include <iostream>
#include <chrono>
#include <boost/uuid/uuid.hpp>
#include <boost/functional/hash.hpp>
#include <kspp/codecs/binary_codec.h>
#include <kspp/topology_builder.h>
#include <kspp/algorithm.h>

static boost::uuids::uuid to_uuid(int64_t x) {
  boost::uuids::uuid uuid;
  memset(uuid.data, 0, 16);
  memcpy(uuid.data, &x, 8);
  return uuid;
}

int main(int argc, char **argv) {
  auto builder     = kspp::topology_builder<kspp::binary_codec>("test_setup", "localhost", "C:\\tmp");
  auto topology = builder.create_topology();

  auto partitioner = [](const boost::uuids::uuid& key)->uint32_t { return boost::hash<boost::uuids::uuid>()(key) % 8; };
  auto table_stream = topology->create_kafka_sink<boost::uuids::uuid, int64_t>("kspp_test0_table", partitioner);
  auto event_stream = topology->create_kafka_sink<boost::uuids::uuid, int64_t>("kspp_test0_eventstream", partitioner);

  std::vector<boost::uuids::uuid> ids;
  for (int i = 0; i != 10000; ++i)
    ids.push_back(to_uuid(i));

  
  std::cerr << "creating " << table_stream->name() << std::endl;
  for (int64_t update_nr = 0; update_nr != 100; ++update_nr) {
    for (auto & i : ids) {
      produce(*table_stream, i, update_nr);
    }
    while (table_stream->queue_len()>10000)
      table_stream->poll(0);
  }

  std::cerr << "creating " << event_stream->name() << std::endl;
  for (int64_t event_nr = 0; event_nr != 100; ++event_nr) {
    for (auto & i : ids) {
      produce(*event_stream, i, event_nr);
    }
    while (event_stream->queue_len()>10000) {
      event_stream->poll(0);
    }
  }
  return 0;
}
