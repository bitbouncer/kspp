#include <iostream>
#include <string>
#include <chrono>
#include <regex>
#include <kspp/codecs/text_codec.h>
#include <kspp/topology_builder.h>
#include <kspp/processors/transform.h>
#include <kspp/processors/count.h>

int main(int argc, char **argv) {
  auto builder = csi::topology_builder<csi::text_codec>("localhost", "C:\\tmp");
  //{
  //  auto sink = builder.create_kafka_sink<int, std::string>("kspp_example5_usernames");
  //  csi::produce<int, std::string>(*sink, 1, "user_1");
  //  csi::produce<int, std::string>(*sink, 2, "user_2");
  //  csi::produce<int, std::string>(*sink, 3, "user_3");
  //  csi::produce<int, std::string>(*sink, 4, "user_4");
  //  csi::produce<int, std::string>(*sink, 5, "user_5");
  //  csi::produce<int, std::string>(*sink, 6, "user_6");
  //  csi::produce<int, std::string>(*sink, 7, "user_7");
  //  csi::produce<int, std::string>(*sink, 8, "user_8");
  //  csi::produce<int, std::string>(*sink, 9, "user_9");
  //  csi::produce<int, std::string>(*sink, 10,"user_10");
  //}
 
  //{
  //  auto sink = builder.create_kafka_sink<int, int>("kspp_example5_user_channel"); // <user_id, channel_id>
  //  csi::produce(*sink, 1, 1);
  //  csi::produce(*sink, 2, 1);
  //  csi::produce(*sink, 3, 1);
  //  csi::produce(*sink, 4, 1);
  //  csi::produce(*sink, 5, 2);
  //  csi::produce(*sink, 6, 2);
  //  csi::produce(*sink, 7, 2);
  //  csi::produce(*sink, 8, 2);
  //  csi::produce(*sink, 9, 2);
  //  csi::produce(*sink, 10, 2);
  //}

  //{
  //  auto sink = builder.create_kafka_sink<int, std::string>("kspp_example5_channel_names");
  //  csi::produce<int, std::string>(*sink, 1, "channel1");
  //  csi::produce<int, std::string>(*sink, 2, "channel2");
  //}

  /*
  {
    auto s0 = builder.create_kafka_sources<int, std::string>("kspp_example5_usernames", 8);
    auto t0 = builder.create_kafka_sources<int, int>("kspp_example5_user_channel", 8);
    builder.create_repartition<int, std::string, int>("example5", s0, t0, [](auto row, auto partition_key) {});
  }
  */



 /* {
    auto sources = builder.create_kafka_sources<int, std::string>("kspp_example5_channel_names", 8);
    for (auto s : sources) {
      std::cerr << s->name() << std::endl;
      s->start(-2);
      while (!s->eof()) {
        auto msg = s->consume();
        if (msg) {
          std::cerr << msg->key << ":" << *msg->value << std::endl;
        }
      }
    }
  }*/

  {
    auto sources = builder.create_kafka_sources<int, std::string>("kspp_example5_usernames", 8);
    for (auto s : sources) {
      std::cerr << s->name() << std::endl;
      s->start(-2);
      while (!s->eof()) {
        auto msg = s->consume();
        if (msg) {
          std::cerr << msg->key << ":" << *msg->value << std::endl;
        }
      }
    }
  }


  auto sink = builder.create_kafka_sink<int, std::string>("kspp_example5_usernames.per-channel");
  for (int i=0; i!=8; ++i)
  {
    auto source = builder.create_kafka_source<int, std::string>("kspp_example5_usernames", i);
    auto routing_table = builder.create_ktable<int, int>("example5", "kspp_example5_user_channel", i);
    auto repartition = std::make_shared<csi::repartition_by_table<int, std::string>>(source, routing_table);
    repartition->add_sink(sink);
    repartition->start(-2);
    while (!repartition->eof()) {
      repartition->consume();
    }
  }


  {
    auto sources = builder.create_kafka_sources<int, std::string>("kspp_example5_usernames.per-channel", 8);
    for (auto s : sources) {
      std::cerr << s->name() << std::endl;
      s->start(-2);
      while (!s->eof()) {
        auto msg = s->consume();
        if (msg) {
          std::cerr << msg->key << ":" << *msg->value << std::endl;
        }
      }
    }
  }
  
  {
    auto sink = builder.create_stream_sink<int, std::string>(std::cerr);
    {
      auto sources = builder.create_kafka_sources<int, std::string>("kspp_example5_usernames.per-channel", 8);
      for (auto s : sources) {
        std::cerr << s->name() << std::endl;
        s->add_sink(sink);
        s->start(-2);
        while (!s->eof())
          s->consume();
        }
      }
    }



  /*
  auto join = builder.create_left_join<int, int64_t, int64_t, int64_t>("join", "kspp_test0_eventstream", "kspp_test0_table", PARTITION, [](const boost::uuids::uuid& key, const int64_t& left, const int64_t& right, int64_t& row) {
    row = right;
  });
  auto sink = builder.create_kafka_sink<boost::uuids::uuid, int64_t>("kspp_test0_eventstream_out", 0);
  join->start();

  // first sync table
  while (join->consume_right()) {
  }
  */

  /*
   auto t0 = std::chrono::high_resolution_clock::now();

  // now join stream with loaded table
  size_t join_count = 0;
  while (!join->eof())
    join_count += consume(*join, *sink);

 */
}
