#include <assert.h>
#include <iostream>
#include <string>
#include <chrono>
#include <kspp/impl/serdes/text_serdes.h>
#include <kspp/topology_builder.h>
#include <kspp/processors/kafka_source.h>
#include <kspp/processors/flat_map.h>
#include <kspp/processors/repartition.h>
#include <kspp/processors/ktable.h>
#include <kspp/state_stores/mem_store.h>
#include <kspp/sinks/kafka_sink.h>
#include <kspp/sinks/stream_sink.h>
#include <kspp/impl/kafka_utils.h>
#include <kspp/utils.h>

using namespace std::chrono_literals;

int main(int argc, char **argv) {
  auto app_info = std::make_shared<kspp::app_info>("kspp-examples", "example5-repartition");
  auto builder = kspp::topology_builder(app_info, kspp::utils::default_kafka_broker_uri(), 100ms);

  {
    auto topology = builder.create_topology();
    auto sink = topology->create_sink<kspp::kafka_sink<int, std::string, kspp::text_serdes>>("kspp_example5_usernames");
    sink->produce(1, "user_1");
    sink->produce(1, "user_1");
    sink->produce(2, "user_2");
    sink->produce(3, "user_3");
    sink->produce(4, "user_4");
    sink->produce(5, "user_5");
    sink->produce(6, "user_6");
    sink->produce(7, "user_7");
    sink->produce(8, "user_8");
    sink->produce(9, "user_9");
    sink->produce(10, "user_10");
  }

  {
    auto topology = builder.create_topology();
    auto sink = topology->create_sink<kspp::kafka_sink<int, int, kspp::text_serdes>>("kspp_example5_user_channel"); // <user_id, channel_id>
    sink->produce(1, 1);
    sink->produce(2, 1);
    sink->produce(3, 1);
    sink->produce(4, 1);
    sink->produce(5, 2);
    sink->produce(6, 2);
    sink->produce(7, 2);
    sink->produce(8, 2);
    sink->produce(9, 2);
    sink->produce(10, 2);
  }

  {
    auto topology = builder.create_topology();
    auto sink = topology->create_sink<kspp::kafka_sink<int, std::string, kspp::text_serdes>>("kspp_example5_channel_names");
    sink->produce(1, "channel1");
    sink->produce(2, "channel2");
  }

  auto partitions1 = kspp::kafka::get_number_partitions(builder.brokers(), "kspp_example5_usernames");
  auto partitions2 = kspp::kafka::get_number_partitions(builder.brokers(), "kspp_example5_user_channel");
  assert(partitions1 == partitions2);

  {
    auto partition_list = kspp::get_partition_list(partitions1);

    auto topology = builder.create_topology();
    auto sources = topology->create_processors<kspp::kafka_source<int, std::string, kspp::text_serdes>>(partition_list, "kspp_example5_usernames");
    topology->create_processors<kspp::stream_sink<int, std::string>>(sources, &std::cerr);
    topology->start(-2);
    topology->flush();

  }

  {
    auto partition_list = kspp::get_partition_list(partitions1);

    auto topology = builder.create_topology();
    auto topic_sink = topology->create_sink<kspp::kafka_sink<int, std::string, kspp::text_serdes>>("kspp_example5_usernames.per-channel");
    auto sources = topology->create_processors<kspp::kafka_source<int, std::string, kspp::text_serdes>>(partition_list, "kspp_example5_usernames");
    auto routing_sources = topology->create_processors<kspp::kafka_source<int, int, kspp::text_serdes>>(partition_list, "kspp_example5_user_channel");
    auto routing_tables = topology->create_processors<kspp::ktable<int, int, kspp::mem_store>>(routing_sources);
    auto repartitions = topology->create_processors<kspp::repartition_by_foreign_key<int, std::string, int, kspp::text_serdes>>(sources, routing_tables, topic_sink);

    topology->init_metrics();
    topology->start(-2);
    topology->flush();
    topology->for_each_metrics([](kspp::metric& m) {
      std::cerr << "metrics: " << m.name() << " : " << m.value() << std::endl;
    });
  }


  {
    auto partitions = kspp::kafka::get_number_partitions(builder.brokers(), "kspp_example5_usernames.per-channel");
    auto partition_list = kspp::get_partition_list(partitions);

    auto topology = builder.create_topology();
    auto sources = topology->create_processors<kspp::kafka_source<int, std::string, kspp::text_serdes>>(partition_list, "kspp_example5_usernames.per-channel");
    topology->create_processors<kspp::stream_sink<int, std::string>>(sources, &std::cerr);
    topology->start(-2);
    topology->flush();
    topology->for_each_metrics([](kspp::metric& m) {
      std::cerr << "metrics: " << m.name() << " : " << m.value() << std::endl;
    });
  }
}
