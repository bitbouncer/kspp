#include <iostream>
#include <string>
#include <chrono>
#include <regex>
#include <kspp/impl/serdes/text_serdes.h>
#include <kspp/topology_builder.h>
#include <kspp/processors/kafka_source.h>
#include <kspp/processors/filter.h>
#include <kspp/processors/flat_map.h>
#include <kspp/processors/pipe.h>
#include <kspp/sinks/stream_sink.h>

#include <kspp/sinks/kafka_sink.h>


using namespace kspp;
using namespace std::chrono_literals;

int main(int argc, char **argv) {
  auto app_info = std::make_shared<kspp::app_info>("kspp-examples", "example6-filter");
  auto builder = topology_builder(app_info, "localhost");
  auto partition_list = kspp::parse_partition_list("[0,1,2,3,4,5,6,7]");
  {
    auto topology = builder.create_topology();
    auto sink = topology->create_sink<kspp::kafka_topic_sink<void, std::string, kspp::text_serdes>>("kspp_TextInput");
    sink->produce("hello kafka streams");
  }

  {
    auto topology = builder.create_topology();
    auto sources = topology->create_processors<kafka_source<void, std::string, text_serdes>>(partition_list, "kspp_TextInput");

    std::regex rgx("\\s+");
    auto word_streams = topology->create_processors<flat_map<void, std::string, std::string, void>>(sources, [&rgx](const auto e, auto flat_map) {
      std::sregex_token_iterator iter(e->value->begin(), e->value->end(), rgx, -1);
      std::sregex_token_iterator end;
      for (; iter != end; ++iter)
        flat_map->push_back(std::make_shared<krecord<std::string, void>>(*iter));
    });

    auto filtered_streams = topology->create_processors<kspp::filter<std::string, void>>(word_streams, [](const auto e)->bool {
      return (e->key != "hello");
    });

    auto mypipes = topology->create_processors<kspp::pipe<std::string, void>>(filtered_streams);
    auto sinks = topology->create_processors<stream_sink<std::string, void>>(mypipes, &std::cerr);
    for (auto i : mypipes)
      i->produce("extra message injected");
    topology->start(-2);
    topology->flush();
  }
}
