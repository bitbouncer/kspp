
#include <iostream>
#include <string>
#include <cstdlib>
#include <cstdio>
#include <csignal>
#include <cstring>
#include <chrono>
#include <kspp/ktable.h>
#include <kspp/kstream.h>
#include <kspp/join.h>
#include <kspp/binary_encoder.h>

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

  auto table_source = std::make_shared<csi::ktable<boost::uuids::uuid, int64_t, csi::binary_codec>>("join", "localhost", "kspp_test0_table", PARTITION, "C:\\tmp", codec);
  auto event_source = std::make_shared<csi::kstream<boost::uuids::uuid, int64_t, csi::binary_codec>>("join", "localhost", "kspp_test0_eventstream", PARTITION, "C:\\tmp", codec);

  std::cerr << "initing table store" << std::endl;
  table_source->start();
  event_source->start(-2);

  while (!table_source->eof()) {
    table_source->consume();
  }
  table_source->commit();
  table_source->flush_offset(); 
  std::cerr << "done - initing table store" << std::endl;

  auto t0 = std::chrono::high_resolution_clock::now();
 
  int64_t join_count = 0;
  int64_t found_count = 0;
  int flush_count = 0;

  while (!event_source->eof()) {
    auto ev = event_source->consume();
    if (ev) {
      join_count++;
      auto row = table_source->get(ev->key);
      if (row)
      {
        found_count++;
      }
    }
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

  //event_source->close();
  //table_source->close();
  //RdKafka::wait_destroyed(5000);
  return 0;
}
