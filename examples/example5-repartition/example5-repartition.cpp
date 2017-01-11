#include <iostream>
#include <string>
#include <chrono>
#include <regex>
#include <kspp/codecs/text_codec.h>
#include <kspp/topology_builder.h>
#include <kspp/processors/transform.h>
#include <kspp/processors/count.h>
#include <kspp/algorithm.h>

int main(int argc, char **argv) {
  auto builder = kspp::topology_builder<kspp::text_codec>("localhost", "C:\\tmp");
  {
    auto sink = builder.create_kafka_sink<int, std::string>("kspp_example5_usernames");
    kspp::produce(*sink, 1, "user_1");
    kspp::produce(*sink, 2, "user_2");
    kspp::produce(*sink, 3, "user_3");
    kspp::produce(*sink, 4, "user_4");
    kspp::produce(*sink, 5, "user_5");
    kspp::produce(*sink, 6, "user_6");
    kspp::produce(*sink, 7, "user_7");
    kspp::produce(*sink, 8, "user_8");
    kspp::produce(*sink, 9, "user_9");
    kspp::produce(*sink, 10, "user_10");
  }

  {
    auto sink = builder.create_kafka_sink<int, int>("kspp_example5_user_channel"); // <user_id, channel_id>
    kspp::produce(*sink, 1, 1);
    kspp::produce(*sink, 2, 1);
    kspp::produce(*sink, 3, 1);
    kspp::produce(*sink, 4, 1);
    kspp::produce(*sink, 5, 2);
    kspp::produce(*sink, 6, 2);
    kspp::produce(*sink, 7, 2);
    kspp::produce(*sink, 8, 2);
    kspp::produce(*sink, 9, 2);
    kspp::produce(*sink, 10, 2);
  }

  {
    auto sink = builder.create_kafka_sink<int, std::string>("kspp_example5_channel_names");
    kspp::produce(*sink, 1, "channel1");
    kspp::produce(*sink, 2, "channel2");
  }

  {
    auto sources = builder.create_kafka_sources<int, std::string>("kspp_example5_usernames", 8);
    auto sink = builder.create_stream_sinks<int, std::string>(sources, std::cerr);
    for (auto s : sources) {
      std::cerr << s->name() << std::endl;
      s->start(-2);
      s->flush();
    }
  }
  
  auto topic_sink = builder.create_kafka_sink<int, std::string>("kspp_example5_usernames.per-channel");
  for (int i = 0; i != 8; ++i) {
    auto partition_source = builder.create_kafka_source<int, std::string>("kspp_example5_usernames", i);
    auto partition_routing_table = builder.create_ktable<int, int>("example5", "kspp_example5_user_channel", i);
    auto partition_repartition = std::make_shared<kspp::repartition_by_table<int, std::string, kspp::text_codec>>(partition_source, partition_routing_table, topic_sink);
    partition_repartition->start(-2);
    partition_repartition->flush();
  }
  
  {
    auto sources = builder.create_kafka_sources<int, std::string>("kspp_example5_usernames.per-channel", 8);
    auto sink = builder.create_stream_sinks<int, std::string>(sources, std::cerr);
    for (auto s : sources) {
      std::cerr << s->name() << std::endl;
      s->start(-2);
      s->flush();
    }
  }
}
