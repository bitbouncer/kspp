#include <iostream>
#include <string>
#include <chrono>
#include <regex>
#include <kspp/impl/serdes/text_serdes.h>
#include <kspp/topology_builder.h>
#include <kspp/processors/flat_map.h>
#include <kspp/processors/count.h>
#include <kspp/state_stores/mem_counter_store.h>
#include <kspp/sources/kafka_source.h>
#include <kspp/sinks/kafka_sink.h>
#include <kspp/utils/kafka_utils.h>
#include <kspp/utils/env.h>

using namespace std::chrono_literals;

#define TOPIC_NAME "kspp_TextInput"

int main(int argc, char **argv) {
  FLAGS_logtostderr = 1;
  google::InitGoogleLogging(argv[0]);

  std::string consumer_group("kspp-examples");
  auto config = std::make_shared<kspp::cluster_config>(consumer_group);
  config->load_config_from_env();
  config->validate();// optional
  config->log(); // optional

  kspp::topology_builder builder(config);
  {
    auto partitions = kspp::kafka::get_number_partitions(config, "kspp_test_text");
    auto partition_list = kspp::get_partition_list(partitions);

    auto topology = builder.create_topology();
    auto sources = topology->create_processors<kspp::kafka_source<void, std::string, void, kspp::text_serdes>>(partition_list, "kspp_test_text");
    std::regex rgx("\\s+");
    auto word_streams = topology->create_processors<kspp::flat_map<void, std::string, std::string, void>>(sources, [&rgx](const auto record, auto stream) {
      std::sregex_token_iterator iter(record.value()->begin(), record.value()->end(), rgx, -1);
      std::sregex_token_iterator end;
      for (; iter != end; ++iter)
        insert(stream, (std::string) *iter);
    });

    auto word_sink = topology->create_sink<kspp::kafka_sink<std::string, void, kspp::text_serdes, void>>("kspp_test_words");
    for (auto i : word_streams)
      i->add_sink(word_sink);

    topology->start(kspp::OFFSET_BEGINNING);
    topology->flush();
  }

  { 
    auto partitions = kspp::kafka::get_number_partitions(config, "kspp_test_words");
    auto partition_list = kspp::get_partition_list(partitions);

    auto topology = builder.create_topology();
    auto word_sources = topology->create_processors<kspp::kafka_source<std::string, void, kspp::text_serdes, void>>(partition_list, "kspp_test_words");
    auto word_counts = topology->create_processors<kspp::count_by_key<std::string, size_t, kspp::mem_counter_store>>(word_sources, 10s);
    
    topology->start(kspp::OFFSET_BEGINNING);
    topology->flush();

    for (auto i : word_counts)
      for (const auto j : *i)
        std::cerr << j->key() << " : " << *j->value() << std::endl;

    topology->for_each_metrics([](kspp::metric& m) {
      std::cerr << "metrics: " << m.name() << " : " << m.value() << std::endl;
    });
  }
}
