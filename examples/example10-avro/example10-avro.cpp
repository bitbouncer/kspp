#include <iostream>
#include <chrono>
#include <kspp/impl/serdes/avro/avro_serdes.h>
#include <kspp/impl/serdes/avro/avro_text.h>
#include <kspp/topology_builder.h>
#include <kspp/sinks/kafka_sink.h>
#include <kspp/sinks/stream_sink.h>
#include <kspp/processors/ktable.h>
#include <kspp/processors/join.h>
#include <kspp/processors/count.h>
#include <kspp/processors/flat_map.h>
#include <kspp/processors/kafka_source.h>
#include <kspp/state_stores/mem_counter_store.h>
#include <kspp/impl/kafka_utils.h>
#include <kspp/utils.h>

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
  auto schema_registry = std::make_shared<kspp::avro_schema_registry>(
          "10.101.100.136:8081,10.101.100.137:8081,10.101.100.138:8081");
  auto avro_serdes = std::make_shared<kspp::avro_serdes>(schema_registry);

  auto app_info = std::make_shared<kspp::app_info>("kspp-examples", "example10-avro");
  auto builder = kspp::topology_builder(app_info, kspp::utils::default_kafka_broker(), 1000ms);
  {
    auto topology = builder.create_topology();
    auto avro_stream = topology->create_sink<kspp::kafka_topic_sink<boost::uuids::uuid, int64_t, kspp::avro_serdes>>(
            "kspp_test10_avro", avro_serdes);

    topology->start(-2);

    std::vector<boost::uuids::uuid> ids;
    for (int i = 0; i != 10; ++i)
      ids.push_back(to_uuid(i));

    std::cerr << "creating " << avro_stream->simple_name() << std::endl;
    for (int64_t update_nr = 0; update_nr != 100; ++update_nr) {
      for (auto &i : ids) {
        avro_stream->produce(i, update_nr);
      }
    }
    topology->flush();
  }

  {
    auto partitions = kspp::kafka::get_number_partitions(builder.brokers(), "kspp_test10_avro");
    auto partition_list = kspp::get_partition_list(partitions);
    auto topology = builder.create_topology();
    auto sources = topology->create_processors<kspp::kafka_source<boost::uuids::uuid, int64_t, kspp::avro_serdes>>(
            partition_list, "kspp_test10_avro", avro_serdes);
    topology->init_metrics();
    topology->start(-2);
    auto t0 = std::chrono::high_resolution_clock::now();
    topology->flush();
    auto t1 = std::chrono::high_resolution_clock::now();
    auto fs1 = t1 - t0;
    auto d1 = std::chrono::duration_cast<std::chrono::milliseconds>(fs1);
    int64_t sz = 0;
    for (auto &&i : sources)
      sz += i->get_metric("in_count");
    std::cout << "typed avro consumes: " << sz << " t: " << d1.count() << "ms\n" << std::endl;
    std::cout << "typed avro per sec : " << 1000.0 * sz / (double) d1.count() << std::endl;

  }

  {
    auto partitions = kspp::kafka::get_number_partitions(builder.brokers(), "kspp_test10_avro");
    auto partition_list = kspp::get_partition_list(partitions);
    auto topology = builder.create_topology();
    auto sources = topology->create_processors<kspp::kafka_source<kspp::GenericAvro, kspp::GenericAvro, kspp::avro_serdes>>(
            partition_list, "kspp_test10_avro", avro_serdes);
    topology->init_metrics();
    topology->start(-2);
    auto t0 = std::chrono::high_resolution_clock::now();
    topology->flush();
    auto t1 = std::chrono::high_resolution_clock::now();
    auto fs1 = t1 - t0;
    auto d1 = std::chrono::duration_cast<std::chrono::milliseconds>(fs1);
    int64_t sz = 0;
    for (auto &&i : sources)
      sz += i->get_metric("in_count");
    std::cout << "generic avro consumes: " << sz << " t: " << d1.count() << "ms\n" << std::endl;
    std::cout << "generic avro per sec : " << 1000.0 * sz / (double) d1.count() << std::endl;

  }

  {
    auto partitions = kspp::kafka::get_number_partitions(builder.brokers(), "kspp_test10_avro");
    auto partition_list = kspp::get_partition_list(partitions);
    auto topology = builder.create_topology();
    auto sources = topology->create_processors<kspp::kafka_source<kspp::GenericAvro, kspp::GenericAvro, kspp::avro_serdes>>(
            partition_list, "kspp_test10_avro", avro_serdes);
    auto sink = topology->create_sink<kspp::kafka_topic_sink<kspp::GenericAvro, kspp::GenericAvro, kspp::avro_serdes>>(
            sources, "kspp_test10_avro_B", avro_serdes);
    topology->init_metrics();
    topology->start(-2);
    auto t0 = std::chrono::high_resolution_clock::now();
    topology->flush();
    auto t1 = std::chrono::high_resolution_clock::now();
    auto fs1 = t1 - t0;
    auto d1 = std::chrono::duration_cast<std::chrono::milliseconds>(fs1);
    int64_t sz = 0;
    for (auto &&i : sources)
      sz += i->get_metric("in_count");
    std::cout << "generic avro read/writes : " << sz << " t: " << d1.count() << "ms\n" << std::endl;
    std::cout << "generic avro read/writes per sec : " << 1000.0 * sz / (double) d1.count() << std::endl;
  }

  // verify that we can read again...
  {
    auto partitions = kspp::kafka::get_number_partitions(builder.brokers(), "kspp_test10_avro_B");
    auto partition_list = kspp::get_partition_list(partitions);
    auto topology = builder.create_topology();
    auto sources = topology->create_processors<kspp::kafka_source<kspp::GenericAvro, kspp::GenericAvro, kspp::avro_serdes>>(
            partition_list, "kspp_test10_avro_B", avro_serdes);
    //auto sink = topology->create_sink<kspp::kafka_topic_sink<kspp::GenericAvro, kspp::GenericAvro, kspp::avro_serdes>>(sources, "kspp_test10_avro_B", avro_serdes);
    topology->init_metrics();
    topology->start(-2);
    auto t0 = std::chrono::high_resolution_clock::now();
    topology->flush();
    auto t1 = std::chrono::high_resolution_clock::now();
    auto fs1 = t1 - t0;
    auto d1 = std::chrono::duration_cast<std::chrono::milliseconds>(fs1);
    int64_t sz = 0;
    for (auto &&i : sources)
      sz += i->get_metric("in_count");
    std::cout << "generic avro read : " << sz << " t: " << d1.count() << "ms\n" << std::endl;
    std::cout << "generic avro read per sec : " << 1000.0 * sz / (double) d1.count() << std::endl;
  }


  return 0;
}
