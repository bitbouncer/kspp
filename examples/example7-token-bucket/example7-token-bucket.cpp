#include <iostream>
#include <string>
#include <chrono>
#include <regex>
#include <kspp/codecs/text_codec.h>
#include <kspp/topology_builder.h>
#include <kspp/processors/filter.h>
#include <kspp/processors/transform.h>
#include <kspp/processors/count.h>
#include <kspp/processors/rate_limiter.h>
#include <kspp/algorithm.h>

#define PARTITION 0

int main(int argc, char **argv) {
  auto builder = kspp::topology_builder<kspp::text_codec>("localhost", "C:\\tmp");
  {
    auto sink = builder.create_kafka_sink<void, std::string>("kspp_TextInput", PARTITION);
    for (int i = 0; i != 100; ++i) {
      kspp::produce<void, std::string>(*sink, "hello kafka streams");
      kspp::produce<void, std::string>(*sink, "more text to parse");
      kspp::produce<void, std::string>(*sink, "even more");
    }
  }

  {
    auto source = builder.create_kafka_source<void, std::string>("kspp_TextInput", PARTITION);

    std::regex rgx("\\s+");
    auto word_stream = std::make_shared<kspp::flat_map<void, std::string, std::string, void>>(source, [&rgx](const auto e, auto flat_map) {
      std::sregex_token_iterator iter(e->value->begin(), e->value->end(), rgx, -1);
      std::sregex_token_iterator end;
      for (; iter != end; ++iter)
        flat_map->push_back(std::make_shared<kspp::krecord<std::string, void>>(*iter));
    });

    auto filtered_stream = builder.create_filter<std::string, void>(word_stream, [](const auto e)->bool {
      return (e->key != "hello");
    });


    auto limited_stream = builder.create_rate_limiter<std::string, void>(filtered_stream, 1000, 10);

    auto word_counts = builder.create_count_by_key<std::string>(limited_stream, 2000); // punctuate every 2 sec

    auto thoughput_limited_stream = builder.create_thoughput_limiter<std::string, size_t>(word_counts, 1);
    auto sink = builder.create_stream_sink<std::string, size_t>(thoughput_limited_stream, std::cerr);

    thoughput_limited_stream->start(-2);
    thoughput_limited_stream->flush();

    // pull everthing through aggregate count - when done punctuate.
    //while (!word_counts->eof()) {
    //  word_counts->process_one();
    //}
    //word_counts->punctuate(kspp::milliseconds_since_epoch());

    while (!thoughput_limited_stream->eof()) {
      thoughput_limited_stream->process_one();
    }


    //thoughput_limited_stream->punctuate(kspp::milliseconds_since_epoch());
  }
}
