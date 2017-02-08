#include <iostream>
#include <string>
#include <chrono>
#include <kspp/codecs/binary_codec.h>
#include <kspp/topology_builder.h>
#include <kspp/processors/kafka_source.h>
#include <kspp/processors/ktable_rocksdb.h>
#include <kspp/processors/join.h>

#define PARTITION 0

int main(int argc, char **argv) {
  size_t join_count = 0;
  auto codec = std::make_shared<kspp::binary_codec>();
  auto builder = kspp::topology_builder("example2-join", "localhost");
  {
    auto topology = builder.create_topology(PARTITION);
    auto stream = topology->create_processor<kspp::kafka_source<boost::uuids::uuid, int64_t, kspp::binary_codec>>("kspp_test0_eventstream", codec);
    auto table_source = topology->create_processor<kspp::kafka_source<boost::uuids::uuid, int64_t, kspp::binary_codec>>("kspp_test0_table", codec);
    auto table = topology->create_processor<kspp::ktable_rocksdb<boost::uuids::uuid, int64_t, kspp::binary_codec>>(table_source,  codec);
    auto join = topology->create_processor<kspp::left_join<boost::uuids::uuid, int64_t, int64_t, int64_t>>(
      stream, 
      table, 
      [&join_count](const boost::uuids::uuid& key, const int64_t& left, const int64_t& right, int64_t& row) {
      row = right;
      join_count++;
    });

    topology->init_metrics();
    topology->start(-2);

    // first sync table
    std::cout << "before sync" << std::endl;
    table->flush();
    table->commit();
    std::cout << "after sync" << std::endl;

    auto t0 = std::chrono::high_resolution_clock::now();

    // now join stream with loaded table
    join_count = 0;
    topology->flush();
    //topology->commit();

    auto t1 = std::chrono::high_resolution_clock::now();
    auto fs = t1 - t0;
    auto d = std::chrono::duration_cast<std::chrono::milliseconds>(fs);
    std::cout << "joins: " << join_count << " t: " << d.count() << "ms\n";
    std::cout << "lookups per sec : " << 1000.0 * join_count / (double)d.count() << std::endl;

    topology->for_each_metrics([](kspp::metric& m) {
      std::cerr << "metrics: " << m.name() << " : " << m.value() << std::endl;
    });
  }
  return 0;
}
