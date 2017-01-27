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
using namespace kspp;

int main(int argc, char **argv) {
  auto codec = std::make_shared<text_codec>();
  auto builder = topology_builder("example6-filter", "localhost");
  {
    auto topology = builder.create_topology(PARTITION);
    auto sink = topology->create<kafka_partition_sink<void, std::string, text_codec>>("kspp_TextInput", codec);
    kspp::produce<void, std::string>(*sink, "hello kafka streams");
  }

  {
    auto topology = builder.create_topology(PARTITION);
    auto source = topology->create<kafka_source<void, std::string, text_codec>>("kspp_TextInput", codec);

    std::regex rgx("\\s+");
    auto word_stream = topology->create<flat_map<void, std::string, std::string, void>>(source, [&rgx](const auto e, auto flat_map) {
      std::sregex_token_iterator iter(e->value->begin(), e->value->end(), rgx, -1);
      std::sregex_token_iterator end;
      for (; iter != end; ++iter)
        flat_map->push_back(std::make_shared<krecord<std::string, void>>(*iter));
    });

    std::shared_ptr<kspp::partition_source<std::string, void>> filtered_stream = topology->create<filter<std::string, void>>(word_stream, [](const auto e)->bool {
      return (e->key != "hello");
    });

    auto mypipe = topology->create<pipe<std::string, void>>(filtered_stream);
    auto sink = topology->create<stream_sink<std::string, void>>(mypipe, &std::cerr);
    mypipe->produce("extra message injected");
    topology->start(-2);
    topology->flush();
  }
}
