#include <iostream>
#include <string>
#include <chrono>
#include <cassert>
#include <kspp/impl/serdes/binary_serdes.h>
#include <kspp/impl/kafka_utils.h>
#include <kspp/topology_builder.h>
#include <kspp/sinks/kafka_sink.h>
#include <kspp/processors/pipe.h>
#include <kspp/processors/kafka_source.h>
#include <kspp/impl/kafka_utils.h>

using namespace std::chrono_literals;

struct record
{
  std::string key;
  std::string value;
};

#define TEST_SIZE 1

int main(int argc, char** argv) {
  kspp::kafka::wait_for_group("localhost", "dummy");

  std::vector<record> test_data;
  for (int i = 0; i != TEST_SIZE; ++i) {
    record r;
    r.key = "key" + std::to_string(i);
    r.value = "value" + std::to_string(i);
    test_data.push_back(r);
  }
 
  auto app_info = std::make_shared<kspp::app_info>("kspp", "test5");
  auto builder = kspp::topology_builder(app_info, "localhost");

  auto nr_of_partitions = kspp::kafka::get_number_partitions(builder.brokers(), "kspp_test5");
  auto partition_list = kspp::get_partition_list(nr_of_partitions);

  /*
  {
    auto consumer_topology = builder.create_topology();
    auto streams = consumer_topology->create_processors<kspp::kafka_source<std::string, std::string, kspp::binary_serdes>>(partition_list, "kspp_test5");
    consumer_topology->start(-1); // end
    consumer_topology->commit(true);
    // now offset should be at end of stream
  }
  */

  {
    auto topology = builder.create_topology();

    auto streams = topology->create_processors<kspp::kafka_source<std::string, std::string, kspp::binary_serdes>>(partition_list, "kspp_test5");
    auto pipe = topology->create_partition_processor<kspp::pipe<std::string, std::string>>();
    auto table_stream = topology->create_sink<kspp::kafka_topic_sink<std::string, std::string, kspp::binary_serdes>>("kspp_test5");
    pipe->add_sink(table_stream);
    
    topology->init(); // remove
    topology->start(-1);

    // insert testdata in pipe
    for (auto & i : test_data) {
      pipe->produce(i.key, i.value);
    }

    std::chrono::time_point<std::chrono::system_clock> end = std::chrono::system_clock::now() + 2000ms;
    while (std::chrono::system_clock::now() < end) {
      topology->flush();
    }
    topology->commit(true);

    assert(table_stream->get_metric("in_count") == TEST_SIZE);

    int64_t sz = 0;
    for (auto&& i : streams)
      sz += i->get_metric("in_count");
    assert(sz == TEST_SIZE);
  }

  // now pick up from last commit
  {
    auto topology = builder.create_topology();

    auto streams = topology->create_processors<kspp::kafka_source<std::string, std::string, kspp::binary_serdes>>(partition_list, "kspp_test5");
    auto pipe = topology->create_partition_processor<kspp::pipe<std::string, std::string>>();
    auto table_stream = topology->create_sink<kspp::kafka_topic_sink<std::string, std::string, kspp::binary_serdes>>("kspp_test5");
    pipe->add_sink(table_stream);

    topology->init(); // remove
    topology->start();

    // insert testdata in pipe
    for (auto & i : test_data) {
      pipe->produce(i.key, i.value);
    }

    std::chrono::time_point<std::chrono::system_clock> end = std::chrono::system_clock::now() + 2000ms;
    while (std::chrono::system_clock::now() < end) {
      topology->flush();
    }
    topology->commit(true);

    assert(table_stream->get_metric("in_count") == TEST_SIZE);

    int64_t sz = 0;
    for (auto&& i : streams)
      sz += i->get_metric("in_count");
    assert(sz == TEST_SIZE);
  }

  return 0;
}