#include <iostream>
#include <string>
#include <chrono>
#include <regex>
#include <kspp/serdes/text_serdes.h>
#include <kspp/topology_builder.h>
#include <kspp/sources/kafka_source.h>
#include <kspp/processors/filter.h>
#include <kspp/processors/flat_map.h>
#include <kspp/processors/count.h>
#include <kspp/processors/rate_limiter.h>
#include <kspp/processors/thoughput_limiter.h>
#include <kspp/sinks/kafka_sink.h>
#include <kspp/sinks/stream_sink.h>
#include <kspp/utils/kafka_utils.h>
#include <kspp/state_stores/mem_counter_store.h>
//#include <kspp/state_stores/mem_windowed_store.h>
//#include <kspp/state_stores/mem_store.h>
#define PARTITION 0

#define TOPIC_NAME "kspp_TextInput"

using namespace std::chrono_literals;

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
    for (int i = 0; i != 100; ++i) {
      sink->push_back("hello kafka streams");
      sink->push_back("more text to parse");
      sink->push_back("even more");
    }
  }

  {
    auto partitions = kspp::kafka::get_number_partitions(config, TOPIC_NAME);
    auto partition_list = kspp::get_partition_list(partitions);


    auto topology = builder.create_topology();
    auto sources = topology->create_processors<kspp::kafka_source<void, std::string, void, kspp::text_serdes>>(
        partition_list, TOPIC_NAME);

    std::regex rgx("\\s+");
    auto word_streams = topology->create_processors<kspp::flat_map<void, std::string, std::string, void>>(sources,
                                                                                                          [&rgx](
                                                                                                              const auto record,
                                                                                                              auto stream) {
                                                                                                            std::sregex_token_iterator iter(
                                                                                                                record.value()->begin(),
                                                                                                                record.value()->end(),
                                                                                                                rgx,
                                                                                                                -1);
                                                                                                            std::sregex_token_iterator end;
                                                                                                            for (;
                                                                                                                iter !=
                                                                                                                end; ++iter)
                                                                                                              insert(
                                                                                                                  stream,
                                                                                                                  (std::string) *iter);
                                                                                                          });

    auto filtered_streams = topology->create_processors<kspp::filter<std::string, void>>(word_streams,
                                                                                         [](const auto record) -> bool {
                                                                                           return (record.key() !=
                                                                                                   "hello");
                                                                                         });

    auto limited_streams = topology->create_processors<kspp::rate_limiter<std::string, void>>(filtered_streams, 1s, 10);
    auto word_counts = topology->create_processors<kspp::count_by_key<std::string, size_t, kspp::mem_counter_store>>(
        limited_streams, 2000s);
    auto thoughput_limited_streams = topology->create_processors<kspp::thoughput_limiter<std::string, size_t>>(
        word_counts, 10); // messages per sec
    auto sinks = topology->create_processors<kspp::stream_sink<std::string, size_t>>(thoughput_limited_streams,
                                                                                     &std::cerr);
    topology->start(kspp::OFFSET_BEGINNING);
    topology->flush();
  }
}
