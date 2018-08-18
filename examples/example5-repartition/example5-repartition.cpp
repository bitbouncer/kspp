#include <assert.h>
#include <iostream>
#include <string>
#include <chrono>
#include <kspp/impl/serdes/text_serdes.h>
#include <kspp/topology_builder.h>
#include <kspp/sources/kafka_source.h>
#include <kspp/processors/flat_map.h>
#include <kspp/processors/repartition.h>
#include <kspp/processors/ktable.h>
#include <kspp/state_stores/mem_store.h>
#include <kspp/sinks/kafka_sink.h>
#include <kspp/sinks/stream_sink.h>
#include <kspp/utils/kafka_utils.h>
#include <kspp/utils/env.h>

using namespace std::chrono_literals;

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
    auto sink = topology->create_sink<kspp::kafka_sink<int, std::string, kspp::text_serdes, kspp::text_serdes>>("kspp_example5_usernames");
    sink->push_back(1, "user_1");
    sink->push_back(1, "user_1");
    sink->push_back(2, "user_2");
    sink->push_back(3, "user_3");
    sink->push_back(4, "user_4");
    sink->push_back(5, "user_5");
    sink->push_back(6, "user_6");
    sink->push_back(7, "user_7");
    sink->push_back(8, "user_8");
    sink->push_back(9, "user_9");
    sink->push_back(10, "user_10");
  }

  {
    auto topology = builder.create_topology();
    auto sink = topology->create_sink<kspp::kafka_sink<int, int, kspp::text_serdes, kspp::text_serdes>>("kspp_example5_user_channel"); // <user_id, channel_id>
    sink->push_back(1, 1);
    sink->push_back(2, 1);
    sink->push_back(3, 1);
    sink->push_back(4, 1);
    sink->push_back(5, 2);
    sink->push_back(6, 2);
    sink->push_back(7, 2);
    sink->push_back(8, 2);
    sink->push_back(9, 2);
    sink->push_back(10, 2);
  }

  {
    auto topology = builder.create_topology();
    auto sink = topology->create_sink<kspp::kafka_sink<int, std::string, kspp::text_serdes, kspp::text_serdes>>("kspp_example5_channel_names");
    sink->push_back(1, "channel1");
    sink->push_back(2, "channel2");
  }

  auto partitions1 = kspp::kafka::get_number_partitions(config, "kspp_example5_usernames");
  auto partitions2 = kspp::kafka::get_number_partitions(config, "kspp_example5_user_channel");
  assert(partitions1 == partitions2);

  {
    auto partition_list = kspp::get_partition_list(partitions1);

    auto topology = builder.create_topology();
    auto sources = topology->create_processors<kspp::kafka_source<int, std::string, kspp::text_serdes, kspp::text_serdes>>(partition_list, "kspp_example5_usernames");
    topology->create_processors<kspp::stream_sink<int, std::string>>(sources, &std::cerr);
    topology->start(kspp::OFFSET_BEGINNING);
    topology->flush();

  }

  {
    auto partition_list = kspp::get_partition_list(partitions1);

    auto topology = builder.create_topology();
    auto topic_sink = topology->create_sink<kspp::kafka_sink<int, std::string, kspp::text_serdes, kspp::text_serdes>>("kspp_example5_usernames.per-channel");
    auto sources = topology->create_processors<kspp::kafka_source<int, std::string, kspp::text_serdes, kspp::text_serdes>>(partition_list, "kspp_example5_usernames");
    auto routing_sources = topology->create_processors<kspp::kafka_source<int, int, kspp::text_serdes, kspp::text_serdes>>(partition_list, "kspp_example5_user_channel");
    auto routing_tables = topology->create_processors<kspp::ktable<int, int, kspp::mem_store>>(routing_sources);
    auto repartitions = topology->create_processors<kspp::repartition_by_foreign_key<int, std::string, int, kspp::text_serdes>>(sources, routing_tables, topic_sink);

    topology->init_metrics();
    topology->start(kspp::OFFSET_BEGINNING);
    topology->flush();
    topology->for_each_metrics([](kspp::metric& m) {
      std::cerr << "metrics: " << m.name() << " : " << m.value() << std::endl;
    });
  }


  {
    auto partitions = kspp::kafka::get_number_partitions(config, "kspp_example5_usernames.per-channel");
    auto partition_list = kspp::get_partition_list(partitions);

    auto topology = builder.create_topology();
    auto sources = topology->create_processors<kspp::kafka_source<int, std::string, kspp::text_serdes, kspp::text_serdes>>(partition_list, "kspp_example5_usernames.per-channel");
    topology->create_processors<kspp::stream_sink<int, std::string>>(sources, &std::cerr);
    topology->init_metrics();
    topology->start(kspp::OFFSET_BEGINNING);
    topology->flush();
    topology->for_each_metrics([](kspp::metric& m) {
      std::cerr << "metrics: " << m.tags() << " " << m.name() << " : " << m.value() << std::endl;
    });
  }
}
