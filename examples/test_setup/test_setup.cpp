
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

static bool run = true;

static void sigterm(int sig) {
  run = false;
}

int main(int argc, char **argv) {
  std::string brokers = "localhost";

  signal(SIGINT, sigterm);
  signal(SIGTERM, sigterm);

  csi::ktable  kt("localhost", "vast-playlist-B1", 0, "C:\\tmp\\ex2");
  csi::kstream ks("localhost", "egress-reporting-queue-B1", 0, "C:\\tmp\\ex2");

  /*while (run) {
    kt.consume();
    if (kt.eof())
      break;
  }*/

  auto t0 = std::chrono::high_resolution_clock::now();
 
  int64_t join_count = 0;
  int64_t found_count = 0;
  while (run) {
    auto msg = ks.consume();
    if (msg) {
      join_count++;
      auto j = kt.find(msg->key_pointer(), msg->key()->size());
      if (j)
        found_count++;
    }
    if (ks.eof())
      break;
  }

  auto t1 = std::chrono::high_resolution_clock::now();
  std::chrono::duration<float> fs = t1 - t0;
  std::chrono::milliseconds  d = std::chrono::duration_cast<std::chrono::milliseconds>(fs);
  std::cout << d.count()/1000 << "s\n";

  std::cout << "lookups per sec : " << join_count / (d.count()/1000) << std::endl;


  /*
  std::cerr << "% Consumed " << msg_cnt << " messages ("
    << msg_bytes << " bytes)" << std::endl;
    */

  std::cerr << "join: " << join_count << " matched " << found_count << std::endl;

  RdKafka::wait_destroyed(5000);
  return 0;
}
