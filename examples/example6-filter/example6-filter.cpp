#include <iostream>
#include <string>
#include <chrono>
#include <regex>
#include <kspp/codecs/text_codec.h>
#include <kspp/topology_builder.h>
#include <kspp/processors/filter.h>
#include <kspp/processors/transform.h>
#include <kspp/processors/count.h>
#include <kspp/algorithm.h>

#define PARTITION 0

int main(int argc, char **argv) {
  auto builder = kspp::topology_builder<kspp::text_codec>("example6-filter", "localhost", "C:\\tmp");
  {
    auto topology = builder.create_topology();
    auto sink = topology->create_kafka_sink<void, std::string>("kspp_TextInput", PARTITION);
    kspp::produce<void, std::string>(*sink, "hello kafka streams");
  }

  {
    auto topology = builder.create_topology();
    auto source = topology->create_kafka_source<void, std::string>("kspp_TextInput", PARTITION);

    std::regex rgx("\\s+");
    auto word_stream = std::make_shared<kspp::flat_map<void, std::string, std::string, void>>(source, [&rgx](const auto e, auto flat_map) {
      std::sregex_token_iterator iter(e->value->begin(), e->value->end(), rgx, -1);
      std::sregex_token_iterator end;
      for (; iter != end; ++iter)
        flat_map->push_back(std::make_shared<kspp::krecord<std::string, void>>(*iter));
    });

    auto filtered_stream = topology->create_filter<std::string, void>(word_stream, [](const auto e)->bool {
      return (e->key != "hello");
    });

    auto pipe = topology->create_pipe<std::string, void>(filtered_stream, PARTITION);
    auto sink = topology->create_stream_sink<std::string, void>(pipe, std::cerr);
    pipe->produce("extra message injected");
    pipe->start(-2);
    pipe->flush();
  }
}
