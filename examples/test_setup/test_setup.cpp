#include <iostream>
#include <chrono>
#include <boost/uuid/uuid.hpp>
#include <boost/functional/hash.hpp>
#include <kspp/impl/serdes/binary_codec.h>
#include <kspp/topology_builder.h>
#include <kspp/sinks/kafka_sink.h>

static boost::uuids::uuid to_uuid(int64_t x) {
  boost::uuids::uuid uuid;
  memset(uuid.data, 0, 16);
  memcpy(uuid.data, &x, 8);
  return uuid;
}

int main(int argc, char **argv) {
  auto codec = std::make_shared<kspp::binary_codec>();
  auto app_info = std::make_shared<kspp::app_info>("kspp-examples", "test_setup");
  auto builder = kspp::topology_builder(app_info, "localhost");
  auto topology = builder.create_partition_topology(-1);

  auto partitioner = [](const boost::uuids::uuid& key)->uint32_t { return boost::hash<boost::uuids::uuid>()(key) % 8; };
  auto table_stream = topology->create_topic_sink<kspp::kafka_topic_sink<boost::uuids::uuid, int64_t, kspp::binary_codec>>("kspp_test0_table", partitioner, codec);
  auto event_stream = topology->create_topic_sink<kspp::kafka_topic_sink<boost::uuids::uuid, int64_t, kspp::binary_codec>>("kspp_test0_eventstream", partitioner, codec);

  topology->init_metrics();

  std::vector<boost::uuids::uuid> ids;
  for (int i = 0; i != 10000; ++i)
    ids.push_back(to_uuid(i));

  std::cerr << "creating " << table_stream->name() << std::endl;
  for (int64_t update_nr = 0; update_nr != 100; ++update_nr) {
    for (auto & i : ids) {
     table_stream->produce(i, update_nr);
    }
    while (table_stream->queue_len() > 10000)
      table_stream->poll(0);
  }

  std::cerr << "creating " << event_stream->name() << std::endl;
  for (int64_t event_nr = 0; event_nr != 100; ++event_nr) {
    for (auto & i : ids) {
      event_stream->produce(i, event_nr);
    }
    while (event_stream->queue_len() > 10000) {
      event_stream->poll(0);
    }
  }
  topology->for_each_metrics([](kspp::metric& m) {
    std::cerr << "metrics: " << m.name() << " : " << m.value() << std::endl;
  });
  return 0;
}
