#include <iostream>
#include <string>
#include <cstdlib>
#include <cstdio>
#include <csignal>
#include <cstring>
#include <chrono>
#include <kspp/binary_encoder.h>
#include <kspp/topology_builder.h>

static bool run = true;

#define PARTITION 0

static void sigterm(int sig) {
  run = false;
}

int main(int argc, char **argv) {
  std::string brokers = "localhost";

  signal(SIGINT, sigterm);
  signal(SIGTERM, sigterm);

  auto codec = std::make_shared<csi::binary_codec>();
  auto builder = csi::topology_builder<csi::binary_codec>("localhost", "C:\\tmp", codec);
  auto t0 = std::chrono::high_resolution_clock::now();
  {
    auto table_source = builder.create_ktable <boost::uuids::uuid, int64_t>("join", "kspp_test0_table", 0);
    auto event_source = builder.create_kstream<boost::uuids::uuid, int64_t>("join", "kspp_test0_eventstream", 0);

    std::cerr << "initing table store" << std::endl;
    table_source->start();
    event_source->start(-2);

    while (!table_source->eof()) {
      table_source->consume();
    }
    table_source->commit();
    table_source->flush_offset();
    std::cerr << "done - initing table store" << std::endl;

    t0 = std::chrono::high_resolution_clock::now();

    int64_t join_count = 0;
    int64_t found_count = 0;
    int flush_count = 0;

    while (!event_source->eof()) {
      auto ev = event_source->consume();
      if (ev) {
        join_count++;
        auto row = table_source->get(ev->key);
        if (row) {
          found_count++;
        }
      }
    }
    auto t1 = std::chrono::high_resolution_clock::now();
    std::chrono::duration<float> fs = t1 - t0;
    std::chrono::milliseconds  d = std::chrono::duration_cast<std::chrono::milliseconds>(fs);
    std::cout << d.count() / 1000 << "s\n";

    std::cout << "lookups per sec : " << join_count / ((double) d.count() / 1000) << std::endl;
  }

  {
    std::cout << std::endl;
    std::cout << std::endl;
    std::cout << "V2 ... " << std::endl;
    std::cout << std::endl;
    
    size_t join_count = 0;
    t0 = std::chrono::high_resolution_clock::now();
    {
      auto join = builder.create_left_join<boost::uuids::uuid, int64_t, int64_t, int64_t>("join", "kspp_test0_eventstream", "kspp_test0_table", PARTITION, [](const boost::uuids::uuid& key, const int64_t& left, const int64_t& right, int64_t& row) {
        row = right;
      });
      auto sink = builder.create_kafka_sink<boost::uuids::uuid, int64_t>("kspp_test0_eventstream_out", 0);
      join_count = 0;
      join->start();
      while (!join->eof()) {
        join_count += consume(*join, *sink);
      }
      join->commit();
    }
    auto t1 = std::chrono::high_resolution_clock::now();

    auto fs = t1 - t0;
    auto d = std::chrono::duration_cast<std::chrono::milliseconds>(fs);
    std::cout << d.count() / 1000 << "s\n";
    std::cout << "lookups per sec : " << join_count / ((double) d.count() / 1000) << std::endl;
  }

  return 0;
}
