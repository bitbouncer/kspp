#include <iostream>
#include <string>
#include <chrono>
#include <regex>
#include <kspp/codecs/text_codec.h>
#include <kspp/topology_builder.h>
#include <kspp/processors/filter.h>
#include <kspp/processors/transform.h>
#include <kspp/processors/count.h>

#define PARTITION 0

int main(int argc, char **argv) {
  auto builder = csi::topology_builder<csi::text_codec>("localhost", "C:\\tmp");
  {
    auto sink = builder.create_kafka_sink<void, std::string>("kspp_TextInput", PARTITION);
    csi::produce<void, std::string>(*sink, "hello kafka streams");
  }

  {
    auto source = builder.create_kafka_source<void, std::string>("kspp_TextInput", PARTITION);
    auto sink = builder.create_stream_sink<void, std::string>(source, std::cerr);
    source->start(-2);
    while (!source->eof()) {
      source->process_one();
    }
  }

  {
    auto source = builder.create_kafka_source<void, std::string>("kspp_TextInput", PARTITION);

    std::regex rgx("\\s+");
    auto word_stream = std::make_shared<csi::flat_map<void, std::string, std::string, void>>(source, [&rgx](const auto e, auto flat_map) {
      std::sregex_token_iterator iter(e->value->begin(), e->value->end(), rgx, -1);
      std::sregex_token_iterator end;
      for (; iter != end; ++iter)
        flat_map->push_back(std::make_shared<csi::krecord<std::string, void>>(*iter));
    });

    auto filtered_stream = builder.create_filter<std::string, void>(word_stream, [](const auto e)->bool {
      return true;
    });
    
    auto sink = builder.create_stream_sink<std::string, void>(filtered_stream, std::cerr);

    filtered_stream->start(-2);
    while (!filtered_stream->eof()) {
      filtered_stream->process_one();
    }
  }
}
