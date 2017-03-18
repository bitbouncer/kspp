#include <iostream>
#include <string>
#include <chrono>
#include <regex>
#include <kspp/impl/serdes/text_serdes.h>
#include <kspp/topology_builder.h>
#include <kspp/processors/flat_map.h>
#include <kspp/processors/count.h>
#include <kspp/state_stores/mem_counter_store.h>
#include <kspp/processors/kafka_source.h>
#include <kspp/sinks/kafka_sink.h>

using namespace std::chrono_literals;

int main(int argc, char **argv) {
  auto app_info = std::make_shared<kspp::app_info>("kspp-examples", "example4-count");
  auto text_builder = kspp::topology_builder(app_info, "localhost");
  auto partition_list = kspp::parse_partition_list("[0,1,2,3,4,5,6,7]"); // tmp fix waiting for partition data in topology_builder.../ topology
  {
    auto topology = text_builder.create_topology();
    auto sources = topology->create_processors<kspp::kafka_source<void, std::string, kspp::text_serdes>>(partition_list, "kspp_test_text");
    std::regex rgx("\\s+");
    auto word_streams = topology->create_processors<kspp::flat_map<void, std::string, std::string, void>>(sources, [&rgx](const auto record, auto flat_map) {
      std::sregex_token_iterator iter(record->value->begin(), record->value->end(), rgx, -1);
      std::sregex_token_iterator end;
      for (; iter != end; ++iter) {
        flat_map->push_back(std::make_shared<kspp::krecord<std::string, void>>(*iter));
      }
    });

    auto word_sink = topology->create_sink<kspp::kafka_topic_sink<std::string, void, kspp::text_serdes>>("kspp_test_words");
    for (auto i : word_streams)
      i->add_sink(word_sink);

    topology->start(-2);
    topology->flush();
  }

  { 
    auto topology = text_builder.create_topology();
    auto word_sources = topology->create_processors<kspp::kafka_source<std::string, void, kspp::text_serdes>>(partition_list, "kspp_test_words");
    auto word_counts = topology->create_processors<kspp::count_by_key<std::string, size_t, kspp::mem_counter_store>>(word_sources, 10s);
    
    topology->init_metrics();
    topology->start(-2);
    topology->flush();

    for (auto i : word_counts)
      for (auto j : *i)
        std::cerr << j->key << " : " << *j->value << std::endl;

    topology->for_each_metrics([](kspp::metric& m) {
      std::cerr << "metrics: " << m.name() << " : " << m.value() << std::endl;
    });
  }
}
