#include <iostream>
#include <string>
#include <chrono>
#include <regex>
#include <kspp/impl/serdes/text_serdes.h>
#include <kspp/topology_builder.h>
#include <kspp/sources/kafka_source.h>
#include <kspp/processors/filter.h>
#include <kspp/processors/flat_map.h>
#include <kspp/processors/generic_stream.h>
#include <kspp/sinks/stream_sink.h>
#include <kspp/sinks/kafka_sink.h>
#include <kspp/utils/kafka_utils.h>
#include <kspp/utils/env.h>

using namespace kspp;
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
    auto topology = builder.create_topology();
    auto sink = topology->create_sink<kspp::kafka_sink<void, std::string, void, kspp::text_serdes>>(TOPIC_NAME);
    sink->push_back("hello kafka streams");
  }

  {
    auto partitions = kspp::kafka::get_number_partitions(config, TOPIC_NAME);
    auto partition_list = kspp::get_partition_list(partitions);


    auto topology = builder.create_topology();
    auto sources = topology->create_processors<kafka_source<void, std::string, void, text_serdes>>(partition_list, TOPIC_NAME);

    std::regex rgx("\\s+");
    auto word_streams = topology->create_processors<flat_map<void, std::string, std::string, void>>(sources, [&rgx](const auto record, auto stream) {
      std::sregex_token_iterator iter(record.value()->begin(), record.value()->end(), rgx, -1);
      std::sregex_token_iterator end;
      for (; iter != end; ++iter)
        insert(stream, (std::string) *iter);
    });

    auto filtered_streams = topology->create_processors<kspp::filter<std::string, void>>(word_streams, [](const auto record)->bool {
      return (record.key() != "hello");
    });

    auto mypipes = topology->create_processors<kspp::generic_stream<std::string, void>>(filtered_streams);
    auto sinks = topology->create_processors<stream_sink<std::string, void>>(mypipes, &std::cerr);
    for (auto i : mypipes)
      i->push_back("extra message injected");
    topology->start(kspp::OFFSET_BEGINNING);
    topology->flush();
  }
}
