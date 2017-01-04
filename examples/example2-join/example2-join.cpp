#include <iostream>
#include <string>
#include <chrono>
#include <kspp/codecs/binary_codec.h>
#include <kspp/topology_builder.h>

#define PARTITION 0

int main(int argc, char **argv) {
  auto builder = csi::topology_builder<csi::binary_codec>("localhost", "C:\\tmp");
  auto stream = builder.create_kafka_source<boost::uuids::uuid, int64_t>("kspp_test0_eventstream", PARTITION);
  auto table = builder.create_ktable<boost::uuids::uuid, int64_t>("join", "kspp_test0_table", PARTITION);
  auto join = std::make_shared<csi::left_join<boost::uuids::uuid, int64_t, int64_t, int64_t>>(stream, table, [](const boost::uuids::uuid& key, const int64_t& left, const int64_t& right, int64_t& row) {
    row = right;
  });
  auto sink = builder.create_kafka_sink<boost::uuids::uuid, int64_t>("kspp_test0_eventstream_out", 0);
  join->start(-2);

  // first sync table
  while (join->consume_right()) {
  }

  auto t0 = std::chrono::high_resolution_clock::now();

  // now join stream with loaded table
  size_t join_count = 0;
  while (!join->eof())
    join->process_one();

  join->commit();

  auto t1 = std::chrono::high_resolution_clock::now();
  auto fs = t1 - t0;
  auto d = std::chrono::duration_cast<std::chrono::milliseconds>(fs);
  std::cout << d.count() / 1000 << "s\n";
  std::cout << "lookups per sec : " << join_count / ((double)d.count() / 1000) << std::endl;
  return 0;
}
