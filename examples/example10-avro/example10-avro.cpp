#include <iostream>
#include <string>
#include <chrono>
#include <kspp/impl/serdes/avro_serdes.h>
#include <kspp/topology_builder.h>
#include <kspp/sinks/kafka_sink.h>
#include <kspp/processors/ktable.h>
#include <kspp/processors/join.h>
#include <kspp/state_stores/mem_store.h>
#include <kspp/impl/kafka_utils.h>

using namespace std::chrono_literals;

static boost::uuids::uuid to_uuid(int64_t x) {
  boost::uuids::uuid uuid;
  memset(uuid.data, 0, 16);
  memcpy(uuid.data, &x, 8);
  return uuid;
}

int main(int argc, char **argv) {
  size_t join_count = 0;

  // maybe we should add http:// here...
  auto schema_registry = std::make_shared<kspp::avro_schema_registry>("10.101.100.136:8081,10.101.100.137:8081,10.101.100.138:8081");
  auto avro_serdes = std::make_shared<kspp::avro_serdes>(schema_registry);

  auto app_info = std::make_shared<kspp::app_info>("kspp-examples", "example10-avro");
  auto builder = kspp::topology_builder(app_info, "10.101.100.136", 1000ms);
  auto topology = builder.create_partition_topology(-1);

  auto avro_stream = topology->create_topic_sink<kspp::kafka_topic_sink<boost::uuids::uuid, int64_t, kspp::avro_serdes>>("kspp_test10_avro", avro_serdes);

  std::vector<boost::uuids::uuid> ids;
  for (int i = 0; i != 1000; ++i)
    ids.push_back(to_uuid(i));

  std::cerr << "creating " << avro_stream->name() << std::endl;
  for (int64_t update_nr = 0; update_nr != 100; ++update_nr) {
    for (auto & i : ids) {
     avro_stream->produce(i, update_nr);
    }
    while (avro_stream->queue_len() > 10000)
      avro_stream->poll(0);
  }

  
  return 0;
}
