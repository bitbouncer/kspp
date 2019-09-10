#include <iostream>
#include <chrono>
#include <kspp/serdes/text_serdes.h>
#include <kspp/topology_builder.h>
#include <kspp/state_stores/mem_store.h>
#include <kspp/sinks/kafka_sink.h>

using namespace std::chrono_literals;

int main(int argc, char **argv) {
  FLAGS_logtostderr = 1;
  google::InitGoogleLogging(argv[0]);

  return 0;
}

