#include <iostream>
#include <chrono>
#include <kspp/impl/serdes/binary_serdes.h>
#include <kspp/topology_builder.h>
#include <kspp/sources/kafka_source.h>
#include <kspp/processors/ktable.h>
#include <kspp/processors/join.h>
#include <kspp/state_stores/mem_store.h>
#include <kspp/impl/kafka_utils.h>
#include <kspp/utils/utils.h>

using namespace std::chrono_literals;

int main(int argc, char **argv) {
  size_t join_count = 0;

  auto config = std::make_shared<kspp::cluster_config>();
  config->set_brokers(kspp::utils::default_kafka_broker_uri());
  config->set_consumer_buffering_time(10ms);
  config->set_producer_buffering_time(10ms);

  auto app_info = std::make_shared<kspp::app_info>("kspp-examples", "example2-join");
  auto builder = kspp::topology_builder(app_info, config);

  auto partitions1 = kspp::kafka::get_number_partitions(config, "kspp_test0_eventstream");
  auto partitions2 = kspp::kafka::get_number_partitions(config, "kspp_test0_table");

  assert(partitions1 == partitions2); // this is a requirement for the join to be meaningfull

  auto partition_list = kspp::get_partition_list(partitions1);

  {
    auto topology = builder.create_topology();
    auto streams = topology->create_processors<kspp::kafka_source<boost::uuids::uuid, int64_t, kspp::binary_serdes>>(
            partition_list, "kspp_test0_eventstream");
    auto table_sources = topology->create_processors<kspp::kafka_source<boost::uuids::uuid, int64_t, kspp::binary_serdes>>(
            partition_list, "kspp_test0_table");
    auto tables = topology->create_processors<kspp::ktable<boost::uuids::uuid, int64_t, kspp::mem_store>>(
            table_sources);
    auto joins = topology->create_processors<kspp::left_join<boost::uuids::uuid, int64_t, int64_t, int64_t>>(
            streams,
            tables,
            [&join_count](const boost::uuids::uuid &key, const int64_t &left, const int64_t &right, int64_t &row) {
              row = right;
              join_count++;
            });

    topology->init_metrics();
    topology->start(kspp::OFFSET_BEGINNING);

    // first sync table
    std::cout << "before sync" << std::endl;
    auto t0 = std::chrono::high_resolution_clock::now();
    for (auto &&i : tables) {
      i->flush();
      i->commit(true);
    }
    auto t1 = std::chrono::high_resolution_clock::now();
    auto fs0 = t1 - t0;
    auto d0 = std::chrono::duration_cast<std::chrono::milliseconds>(fs0);
    std::cout << "after sync" << " t: " << d0.count() << "ms\n" << std::endl;

    size_t items_in_tables = 0;
    for (auto &&i : tables)
      for (const auto &&j : *i)
        ++items_in_tables;

    size_t table_updates = 0;
    for (auto &&i : tables) {
      table_updates += i->get_metric("in_count");
    }

    std::cout << "inserts : " << table_updates << std::endl;
    std::cout << "table rows : " << items_in_tables << std::endl;
    std::cout << "inserts per sec : " << 1000.0 * table_updates / (double) d0.count() << std::endl;

    auto t2 = std::chrono::high_resolution_clock::now();
    // now join stream with loaded table
    join_count = 0;
    topology->flush();
    //topology->commit();

    auto t3 = std::chrono::high_resolution_clock::now();
    auto fs1 = t3 - t2;
    auto d1 = std::chrono::duration_cast<std::chrono::milliseconds>(fs1);
    std::cout << "joins: " << join_count << " t: " << d1.count() << "ms\n";
    std::cout << "lookups per sec : " << 1000.0 * join_count / (double) d1.count() << std::endl;

    topology->for_each_metrics([](kspp::metric &m) {
      std::cerr << "metrics: " << m.name() << " : " << m.value() << std::endl;
    });
  }
  return 0;
}
