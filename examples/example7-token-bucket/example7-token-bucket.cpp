#include <iostream>
#include <string>
#include <chrono>
#include <regex>
#include <kspp/impl/serdes/text_codec.h>
#include <kspp/topology_builder.h>
#include <kspp/processors/kafka_source.h>
#include <kspp/processors/filter.h>
#include <kspp/processors/flat_map.h>
#include <kspp/processors/count.h>
#include <kspp/processors/rate_limiter.h>
#include <kspp/processors/thoughput_limiter.h>
#include <kspp/sinks/kafka_sink.h>
#include <kspp/sinks/stream_sink.h>
#include <kspp/state_stores/mem_counter_store.h>

#define PARTITION 0

using namespace std::chrono_literals;

int main(int argc, char **argv) {
  auto codec = std::make_shared<kspp::text_codec>();
  auto app_info = std::make_shared<kspp::app_info>("kspp-examples", "example7-token-bucket");
  auto builder = kspp::topology_builder(app_info, "localhost");
  auto partition_list = kspp::parse_partition_list("[0,1,2,3,4,5,6,7]");

  {
    auto topology = builder.create_topology();
    auto sink = topology->create_sink<kspp::kafka_topic_sink<void, std::string, kspp::text_codec>>("kspp_TextInput", codec);
    for (int i = 0; i != 100; ++i) {
      sink->produce("hello kafka streams");
      sink->produce("more text to parse");
      sink->produce("even more");
    }
  }

  {
    auto topology = builder.create_topology();
    auto sources = topology->create_processors<kspp::kafka_source<void, std::string, kspp::text_codec>>(partition_list, "kspp_TextInput", codec);

    std::regex rgx("\\s+");
    auto word_streams = topology->create_processors<kspp::flat_map<void, std::string, std::string, void>>(sources, [&rgx](const auto e, auto flat_map) {
      std::sregex_token_iterator iter(e->value->begin(), e->value->end(), rgx, -1);
      std::sregex_token_iterator end;
      for (; iter != end; ++iter)
        flat_map->push_back(std::make_shared<kspp::krecord<std::string, void>>(*iter));
    });

    auto filtered_streams = topology->create_processors<kspp::filter<std::string, void>>(word_streams, [](const auto e)->bool {
      return (e->key != "hello");
    });

    auto limited_streams = topology->create_processors<kspp::rate_limiter<std::string, void>>(filtered_streams, 1s, 10);
    auto word_counts = topology->create_processors<kspp::count_by_key<std::string, size_t, kspp::mem_counter_store>>(limited_streams, 2000s);
    auto thoughput_limited_streams = topology->create_processors<kspp::thoughput_limiter<std::string, size_t>>(word_counts, 10); // messages per sec
    auto sinks = topology->create_processors<kspp::stream_sink<std::string, size_t>>(thoughput_limited_streams, &std::cerr);
    topology->start(-2);
    topology->flush();
  }
}
