
#include <iostream>
#include <string>
#include <cstdlib>
#include <cstdio>
#include <csignal>
#include <cstring>
#include <chrono>
#include <kspp/ktable.h>
#include <kspp/kstream.h>
#include <kspp/join_processor.h>
#include <kspp/encoder.h>

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
  //auto partitioner = [](const boost::uuids::uuid& key, const int64_t& value)->uint32_t { return value % 8; };

  csi::ktable2<boost::uuids::uuid, int64_t, csi::binary_codec>  table_source("localhost", "kspp_test0_table", PARTITION, "C:\\tmp\\join", codec);
  csi::kstream2<boost::uuids::uuid, int64_t, csi::binary_codec> event_source("localhost", "kspp_test0_eventstream", PARTITION, "C:\\tmp\\join", codec);

  //csi::kafka_producer2<boost::uuids::uuid, int64_t, csi::binary_codec>  event_stream("localhost", "kspp_test0_eventstream", codec, partitioner);
  //csi::ktable  kt("localhost", "vast-playlist-B1", 0, "C:\\tmp\\ex2");
  //csi::kstream ks("localhost", "egress-reporting-queue-B1", 0, "C:\\tmp\\ex2");

  std::cerr << "initing table store" << std::endl;
  while (run) {
    table_source.consume();
    if (table_source.eof())
      break;
  }
  std::cerr << "done - initing table store" << std::endl;

  auto t0 = std::chrono::high_resolution_clock::now();
 
  int64_t join_count = 0;
  int64_t found_count = 0;
  while (run) {
    auto ev = event_source.consume();
    if (ev) {
      join_count++;
      auto row = table_source.find(ev->key_pointer(), ev->key_len());
      if (row)
      {
        found_count++;
        auto e = event_source.parse(ev);
        auto r = table_source.parse(row);
        //e->value->
      }
    }
    if (event_source.eof())
      break;
  }

  auto t1 = std::chrono::high_resolution_clock::now();
  std::chrono::duration<float> fs = t1 - t0;
  std::chrono::milliseconds  d = std::chrono::duration_cast<std::chrono::milliseconds>(fs);
  std::cout << d.count()/1000 << "s\n";

  std::cout << "lookups per sec : " << join_count / ((double) d.count()/1000) << std::endl;


  /*
  std::cerr << "% Consumed " << msg_cnt << " messages ("
    << msg_bytes << " bytes)" << std::endl;
    */

  std::cerr << "join: " << join_count << " matched " << found_count << std::endl;

  event_source.close();
  table_source.close();

  //RdKafka::wait_destroyed(5000);
  return 0;
}
