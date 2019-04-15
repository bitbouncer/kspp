#include <iostream>
#include <string>
#include <chrono>
#include <regex>
#include <kspp/impl/serdes/text_serdes.h>
#include <kspp/topology_builder.h>
#include <kspp/processors/generic_stream.h>
#include <kspp/sources/kafka_source.h>
#include <kspp/processors/flat_map.h>
#include <kspp/processors/count.h>
#include <kspp/state_stores/mem_counter_store.h>
#include <kspp/sinks/kafka_sink.h>
#include <kspp/sinks/stream_sink.h>
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
  config->validate();
  config->log();

  kspp::topology_builder builder(config);
  {
    auto topology = builder.create_topology();
    auto sink = topology->create_sink<kspp::kafka_sink<void, std::string, void, kspp::text_serdes>>(TOPIC_NAME);
    sink->push_back("hello kafka streams");
  }

  auto partitions = kspp::kafka::get_number_partitions(config, TOPIC_NAME);
  auto partition_list = kspp::get_partition_list(partitions);

  {
    auto topology = builder.create_topology();
    auto source = topology->create_processors<kspp::kafka_source<void, std::string, void, kspp::text_serdes>>(partition_list, TOPIC_NAME);
    std::regex rgx("\\s+");
    auto word_stream = topology->create_processors<kspp::flat_map<void, std::string, std::string, void>>(
        source,
        [&rgx](const auto record, auto stream) {
          std::sregex_token_iterator iter(record.value()->begin(), record.value()->end(), rgx, -1);
          std::sregex_token_iterator end;
          for (; iter != end; ++iter)
            insert(stream, (std::string) *iter);
        });

    auto word_counts = topology->create_processors<kspp::count_by_key<std::string, int, kspp::mem_counter_store>>(
            word_stream, 2s);


    auto merged = topology->create_processor<kspp::merge<std::string, int>>();

    for (auto& i : word_counts)
      merged->add(*i);

    auto sink = topology->create_processor<kspp::stream_sink<std::string, int>>(merged, &std::cerr);

    topology->start(kspp::OFFSET_BEGINNING);
    topology->flush();
  }
}
