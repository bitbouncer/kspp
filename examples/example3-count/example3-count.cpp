#include <iostream>
#include <string>
#include <chrono>
#include <regex>
#include <kspp/impl/serdes/text_serdes.h>
#include <kspp/topology_builder.h>
#include <kspp/processors/flat_map.h>
#include <kspp/processors/count.h>
#include <kspp/processors/kafka_source.h>
#include <kspp/state_stores/mem_counter_store.h>
#include <kspp/sinks/kafka_sink.h>
#include <kspp/sinks/stream_sink.h>

using namespace std::chrono_literals;

int main(int argc, char **argv) {
  auto app_info = std::make_shared<kspp::app_info>("kspp-examples", "example3-count");
  auto builder = kspp::topology_builder(app_info, "localhost");
  auto partition_list = kspp::parse_partition_list("[0,1,2,3,4,5,6,7]");
  {
    auto topology = builder.create_topology();
    auto sink = topology->create_sink<kspp::kafka_topic_sink<void, std::string, kspp::text_serdes>>("kspp_TextInput");
    sink->produce("hello kafka streams");
  }

  {
    auto topology = builder.create_topology();
    auto source = topology->create_processors<kspp::kafka_source<void, std::string, kspp::text_serdes>>(partition_list, "kspp_TextInput");

    std::regex rgx("\\s+");
    auto word_stream = topology->create_processors<kspp::flat_map<void, std::string, std::string, void>>(source, [&rgx](const auto record, auto flat_map) {
      std::sregex_token_iterator iter(record->value->begin(), record->value->end(), rgx, -1);
      std::sregex_token_iterator end;
      for (; iter != end; ++iter) {
        flat_map->push_back(std::make_shared<kspp::krecord<std::string, void>>(*iter));
      }
    });

    auto word_counts = topology->create_processors<kspp::count_by_key<std::string, int, kspp::mem_counter_store>>(word_stream, 2s);
    auto sink = topology->create_processors<kspp::stream_sink<std::string, int>>(word_counts, &std::cerr);

    topology->init_metrics();
    topology->start(-2);
    topology->flush();
  }
}
