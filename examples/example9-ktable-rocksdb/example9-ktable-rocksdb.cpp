#include <iostream>
#include <string>
#include <chrono>
#include <regex>
#include <kspp/impl/serdes/text_serdes.h>
#include <kspp/impl/serdes/binary_serdes.h>
#include <kspp/topology_builder.h>
#include <kspp/processors/kafka_source.h>
#include <kspp/processors/ktable.h>
#include <kspp/processors/filter.h>
#include <kspp/processors/flat_map.h>
#include <kspp/processors/count.h>
#include <kspp/processors/rate_limiter.h>
#include <kspp/sinks/kafka_sink.h>
#include <kspp/sinks/stream_sink.h>

#include <kspp/state_stores/rocksdb_counter_store.h>
#include <kspp/state_stores/rocksdb_store.h>
#include <kspp/state_stores/mem_store.h>
#include <kspp/state_stores/mem_windowed_store.h>

using namespace std::chrono_literals;

int main(int argc, char **argv) {
  auto app_info = std::make_shared<kspp::app_info>("kspp-examples", "example8-ktable-mem");
  auto builder = kspp::topology_builder(app_info, "localhost");
  auto partition_list = kspp::parse_partition_list("[0,1,2,3,4,5,6,7]");
  {
    auto topology = builder.create_topology();
    auto sink = topology->create_sink<kspp::kafka_topic_sink<void, std::string, kspp::text_serdes>>("kspp_TextInput");
    for (int i = 0; i != 100; ++i) {
      sink->produce("hello kafka streams");
      sink->produce("more text to parse");
      sink->produce("even more");
    }
  }

  {
    auto topology = builder.create_topology();
    auto sources = topology->create_processors<kspp::kafka_source<void, std::string, kspp::text_serdes>>(partition_list, "kspp_TextInput");

    std::regex rgx("\\s+");
    auto word_streams = topology->create_processors<kspp::flat_map<void, std::string, std::string, void>>(sources, [&rgx](const auto trans, auto flat_map) {
      std::sregex_token_iterator iter(trans->record->value->begin(), trans->record->value->end(), rgx, -1);
      std::sregex_token_iterator end;
      for (; iter != end; ++iter) {
        auto t1 = std::make_shared<kspp::ktransaction<std::string, void>>(std::make_shared<kspp::krecord<std::string, void>>(*iter));
        t1->_commit_callback = trans->_commit_callback; // keep transaction running...
        flat_map->push_back(t1);
      }
    });

    auto filtered_streams = topology->create_processors<kspp::filter<std::string, void>>(word_streams, [](const auto e)->bool {
      return (e->key != "hello");
    });
    
    auto binary_serdes = std::make_shared<kspp::binary_serdes>();

    // this should be possible to do in memory
    auto word_counts = topology->create_processors<kspp::count_by_key<std::string, int64_t, kspp::rocksdb_counter_store, kspp::binary_serdes>>(filtered_streams, 100ms, binary_serdes);
   
    auto ex1 = topology->create_processors<kspp::ktable<std::string, int64_t, kspp::rocksdb_store, kspp::binary_serdes>>(word_counts, binary_serdes);
    auto ex2 = topology->create_processors<kspp::ktable<std::string, int64_t, kspp::mem_store>>(word_counts);
    auto ex3 = topology->create_processors<kspp::ktable<std::string, int64_t, kspp::mem_windowed_store>>(word_counts, 500ms, 10);

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
