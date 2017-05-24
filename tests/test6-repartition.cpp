#include <iostream>
#include <string>
#include <chrono>
#include <kspp/impl/serdes/text_serdes.h>
#include <kspp/topology_builder.h>
#include <kspp/processors/kafka_source.h>
#include <kspp/processors/flat_map.h>
#include <kspp/processors/repartition.h>
#include <kspp/processors/ktable.h>
#include <kspp/state_stores/mem_store.h>
#include <kspp/sinks/kafka_sink.h>
#include <kspp/sinks/stream_sink.h>
#include <assert.h>
#include <kspp/impl/kafka_utils.h>

using namespace std::chrono_literals;

int main(int argc, char **argv) {
  return 0;
}

