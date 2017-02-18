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

using namespace std::chrono_literals;

int main(int argc, char **argv) {
  auto codec = std::make_shared<kspp::text_codec>();
  auto app_info = std::make_shared<kspp::app_info>("kspp-examples", "example8-ktable-mem");
  auto builder = kspp::topology_builder(app_info, "localhost");
  auto partition_list = kspp::parse_partition_list("[0,1,2,3,4,5,6,7]");
  {
    auto topology = builder.create_topic_topology();
    auto sink = topology->create_topic_sink<kspp::kafka_topic_sink<void, std::string, kspp::text_codec>>("kspp_TextInput", codec);
    for (int i = 0; i != 100; ++i) {
      sink->produce2("hello kafka streams");
      sink->produce2("more text to parse");
      sink->produce2("even more");
    }
  }

  {
    auto topology = builder.create_topic_topology();
    auto sources = topology->create_partition_processors<kspp::kafka_source<void, std::string, kspp::text_codec>>(partition_list, "kspp_TextInput", codec);

    std::regex rgx("\\s+");
    auto word_streams = topology->create_partition_processors<kspp::flat_map<void, std::string, std::string, void>>(sources, [&rgx](const auto e, auto flat_map) {
      std::sregex_token_iterator iter(e->value->begin(), e->value->end(), rgx, -1);
      std::sregex_token_iterator end;
      for (; iter != end; ++iter)
        flat_map->push_back(std::make_shared<kspp::krecord<std::string, void>>(*iter));
    });

    auto filtered_streams = topology->create_partition_processors<kspp::filter<std::string, void>>(word_streams, [](const auto e)->bool {
      return (e->key != "hello");
    });
    
    auto binary_codec = std::make_shared<kspp::binary_codec>();

    // this should be possible to do in memory
    auto word_counts = topology->create_partition_processors<kspp::count_by_key<std::string, int64_t, kspp::binary_codec>>(filtered_streams, 100ms, binary_codec);
   
    auto ex1 = topology->create_partition_processors<kspp::ktable<std::string, int64_t, kspp::rocksdb_store, kspp::binary_codec>>(word_counts, binary_codec);
    auto ex2 = topology->create_partition_processors<kspp::ktable<std::string, int64_t, kspp::mem_store>>(word_counts);
    auto ex3 = topology->create_partition_processors<kspp::ktable<std::string, int64_t, kspp::mem_windowed_store>>(word_counts, 500ms, 10);

    topology->start();
    topology->flush();

    std::cerr << "using range iterators " << std::endl;
    for (auto&& i : ex1)
      for (auto&& j : *i)
        std::cerr << "item : " << j->key << ": " << *j->value << std::endl;

    std::cerr << "using range iterators " << std::endl;
    for (auto&& i : ex1)
      for (auto&& j : *i)
        std::cerr << "item : " << j->key << ": " << *j->value << std::endl;

    std::cerr << "using iterators " << std::endl;
    for (auto&& i : ex2)
      for (auto&& j = i->begin(), end = i->end(); j != end; ++j)
        std::cerr << "item : " << (*j)->key << ": " << *(*j)->value << std::endl;

    std::cerr << "using range iterators " << std::endl;
    for (auto&& i : ex3)
      for (auto&& j : *i)
        std::cerr << "item : " << j->key << ": " << *j->value << std::endl;
  }
}
