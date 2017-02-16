#include <iostream>
#include <string>
#include <chrono>
#include <regex>
#include <kspp/codecs/text_codec.h>
#include <kspp/topology_builder.h>
#include <kspp/processors/kafka_source.h>
#include <kspp/processors/filter.h>
#include <kspp/processors/transform.h>
#include <kspp/processors/count.h>
#include <kspp/processors/rate_limiter.h>
#include <kspp/algorithm.h>
#include <kspp/sinks/kafka_sink.h>
#include <kspp/sinks/stream_sink.h>

#define PARTITION 0

using namespace std::chrono_literals;

int main(int argc, char **argv) {
  auto codec = std::make_shared<kspp::text_codec>();
  auto app_info = std::make_shared<kspp::app_info>("kspp-examples", "example7-token-bucket");
  auto builder = kspp::topology_builder(app_info, "localhost");
  {
    auto topology = builder.create_topology(PARTITION);
    auto sink = topology->create_processor<kspp::kafka_partition_sink<void, std::string, kspp::text_codec>>("kspp_TextInput", codec);
    for (int i = 0; i != 100; ++i) {
      kspp::produce<void, std::string>(*sink, "hello kafka streams");
      kspp::produce<void, std::string>(*sink, "more text to parse");
      kspp::produce<void, std::string>(*sink, "even more");
    }
  }

  {
    auto topology = builder.create_topology(PARTITION);
    auto source = topology->create_processor<kspp::kafka_source<void, std::string, kspp::text_codec>>("kspp_TextInput", codec);

    std::regex rgx("\\s+");
    auto word_stream = topology->create_processor<kspp::flat_map<void, std::string, std::string, void>>(source, [&rgx](const auto e, auto flat_map) {
      std::sregex_token_iterator iter(e->value->begin(), e->value->end(), rgx, -1);
      std::sregex_token_iterator end;
      for (; iter != end; ++iter)
        flat_map->push_back(std::make_shared<kspp::krecord<std::string, void>>(*iter));
    });

    auto filtered_stream = topology->create_processor<kspp::filter<std::string, void>>(word_stream, [](const auto e)->bool {
      return (e->key != "hello");
    });

    auto limited_stream = topology->create_processor<kspp::rate_limiter<std::string, void>>(filtered_stream, 1s, 10);

    auto word_counts = topology->create_processor<kspp::count_by_key<std::string, size_t, kspp::text_codec>>(limited_stream, 2000s, codec);
    
    auto thoughput_limited_stream = topology->create_processor<kspp::thoughput_limiter<std::string, size_t>>(word_counts, 10); // messages per sec
    
    auto sink = topology->create_processor<kspp::stream_sink<std::string, size_t>>(thoughput_limited_stream, &std::cerr);

    thoughput_limited_stream->start(-2);
    thoughput_limited_stream->flush();
  }
}
