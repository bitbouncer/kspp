#include <iostream>
#include <string>
#include <chrono>
#include <regex>
#include <kspp/codecs/text_codec.h>
#include <kspp/codecs/binary_codec.h>
#include <kspp/topology_builder.h>
#include <kspp/processors/kafka_source.h>
#include <kspp/processors/ktable.h>
#include <kspp/processors/filter.h>
#include <kspp/processors/transform.h>
#include <kspp/processors/count.h>
#include <kspp/processors/rate_limiter.h>
#include <kspp/algorithm.h>
#include <kspp/sinks/kafka_sink.h>
#include <kspp/sinks/stream_sink.h>

#include <kspp/state_stores/rocksdb_store.h>
#include <kspp/state_stores/mem_store.h>
#include <kspp/state_stores/mem_windowed_store.h>

#define PARTITION 0

using namespace std::chrono_literals;

int main(int argc, char **argv) {
  auto codec = std::make_shared<kspp::text_codec>();
  auto app_info = std::make_shared<kspp::app_info>("kspp-examples", "example8-ktable-mem");
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
    
    auto binary_codec = std::make_shared<kspp::binary_codec>();

    // this should be possible to do in memory
    auto word_counts = topology->create_processor<kspp::count_by_key<std::string, int64_t, kspp::binary_codec>>(filtered_stream, 100ms, binary_codec);
   
    auto table1 = topology->create_processor<kspp::ktable<std::string, int64_t, kspp::rockdb_store, kspp::binary_codec>>(word_counts, binary_codec);
    auto table2 = topology->create_processor<kspp::ktable<std::string, int64_t, kspp::mem_store>>(word_counts);
    auto table3 = topology->create_processor<kspp::ktable<std::string, int64_t, kspp::mem_windowed_store>>(word_counts, 500ms, 10);

    topology->start();
    topology->flush();

    std::cerr << "using range iterators " << std::endl;
    for (auto&& i : *table1)
      std::cerr << "item : " << i->key << ": " << *i->value << std::endl;

    std::cerr << "using range iterators " << std::endl;
    for (auto&& i : *table2)
      std::cerr << "item : " << i->key << ": " << *i->value << std::endl;

    std::cerr << "using iterators " << std::endl;
    for (auto&& i = table3->begin(), end = table3->end(); i != end; ++i)
      std::cerr << "item : " << (*i)->key << ": " << *(*i)->value << std::endl;

    std::cerr << "using range iterators " << std::endl;
    for (auto&& i : *table3)
      std::cerr << "item : " << i->key << ": " << *i->value << std::endl;
  }
}
