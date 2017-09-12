#include <iostream>
#include <string>
#include <chrono>
#include <regex>
#include <kspp/impl/serdes/text_serdes.h>
#include <kspp/topology_builder.h>
#include <kspp/sources/pipe.h>
#include <kspp/sources/kafka_source.h>
#include <kspp/processors/flat_map.h>
#include <kspp/processors/count.h>
#include <kspp/state_stores/mem_counter_store.h>
#include <kspp/sinks/kafka_sink.h>
#include <kspp/sinks/stream_sink.h>
#include <kspp/impl/kafka_utils.h>
#include <kspp/utils/env.h>

using namespace std::chrono_literals;

#define TOPIC_NAME "kspp_TextInput"

int main(int argc, char **argv) {
  FLAGS_logtostderr = 1;
  google::InitGoogleLogging(argv[0]);

  auto config = std::make_shared<kspp::cluster_config>();
  config->load_config_from_env();
  config->validate();// optional
  config->log(); // optional

  auto builder = kspp::topology_builder("kspp-examples", argv[0], config);
  {
    auto topology = builder.create_topology();
    auto sink = topology->create_sink<kspp::kafka_sink<void, std::string, kspp::text_serdes>>(TOPIC_NAME);
    sink->produce("hello kafka streams");
  }

  auto partitions = kspp::kafka::get_number_partitions(config, TOPIC_NAME);
  auto partition_list = kspp::get_partition_list(partitions);

  {
    auto topology = builder.create_topology();
    auto source = topology->create_processors<kspp::kafka_source<void, std::string, kspp::text_serdes>>(partition_list,
                                                                                                        TOPIC_NAME);
    std::regex rgx("\\s+");
    auto word_stream = topology->create_processors<kspp::flat_map<void, std::string, std::string, void>>(
        source,
        [&rgx](const auto record, auto flat_map) {
          std::sregex_token_iterator iter(record->value()->begin(), record->value()->end(), rgx, -1);
          std::sregex_token_iterator end;
          for (; iter != end; ++iter) {
            flat_map->push_back(std::make_shared<kspp::krecord<std::string, void>>(*iter));
          }
        });

    auto word_counts = topology->create_processors<kspp::count_by_key<std::string, int, kspp::mem_counter_store>>(
            word_stream, 2s);

    auto merged = topology->merge(word_counts);

    auto sink = topology->create_processor<kspp::stream_sink<std::string, int>>(merged, &std::cerr);

    topology->init_metrics();
    topology->start(kspp::OFFSET_BEGINNING);
    topology->flush();
  }
}
