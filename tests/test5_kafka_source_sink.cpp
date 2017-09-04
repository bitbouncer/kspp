#include <iostream>
#include <chrono>
#include <cassert>
#include <kspp/impl/serdes/binary_serdes.h>
#include <kspp/impl/kafka_utils.h>
#include <kspp/topology_builder.h>
#include <kspp/sinks/kafka_sink.h>
#include <kspp/sources/pipe.h>
#include <kspp/state_stores/mem_store.h>
#include <kspp/sources/kafka_source.h>
#include <kspp/processors/ktable.h>

using namespace std::chrono_literals;

struct record {
  std::string key;
  std::string value;
};

#define TEST_SIZE 10

int main(int argc, char **argv) {

  std::vector<record> test_data;
  for (int i = 0; i != TEST_SIZE; ++i) {
    record r;
    r.key = "key" + std::to_string(i);
    r.value = "value" + std::to_string(i);
    test_data.push_back(r);
  }

  auto config = std::make_shared<kspp::cluster_config>();
  config->set_brokers("localhost");
  config->set_consumer_buffering_time(10ms);
  config->set_producer_buffering_time(10ms);
  config->validate();

  kspp::kafka::wait_for_group(config, "dummy");

  auto app_info = std::make_shared<kspp::app_info>("kspp", "test5");
  auto builder = kspp::topology_builder(app_info, config);

  auto nr_of_partitions = kspp::kafka::get_number_partitions(config, "kspp_test5");
  auto partition_list = kspp::get_partition_list(nr_of_partitions);

  auto t0 = kspp::milliseconds_since_epoch();
  {
    auto topology = builder.create_topology();

    auto streams = topology->create_processors<kspp::kafka_source<std::string, std::string, kspp::binary_serdes>>(
        partition_list, "kspp_test5");
    auto ktables = topology->create_processors<kspp::ktable<std::string, std::string, kspp::mem_store>>(streams);

    auto pipe = topology->create_processor<kspp::pipe<std::string, std::string>>(-1);
    auto table_stream = topology->create_sink<kspp::kafka_sink<std::string, std::string, kspp::binary_serdes>>(
        "kspp_test5");
    pipe->add_sink(table_stream);

    topology->init(); // remove
    topology->start(-1);

    // insert testdata in pipe
    for (auto &i : test_data) {
      pipe->produce(i.key, i.value, t0);
    }

    std::chrono::time_point<std::chrono::system_clock> end = std::chrono::system_clock::now() + 2000ms;
    while (std::chrono::system_clock::now() < end) {
      topology->flush();
    }
    topology->commit(true);

    assert(table_stream->get_metric("in_count") == TEST_SIZE);

    int64_t sz = 0;
    for (auto &&i : streams)
      sz += i->get_metric("in_count");
    LOG(INFO) << "sz: " << sz << " expected : " <<  TEST_SIZE;
    assert(sz == TEST_SIZE);

    // verify timestamps on all elements in ktable
    for (auto &&i : ktables)
      for (auto &&j : *i) {
        auto ts = j->event_time();
        assert(ts == t0);
      }
  }

  // now pick up from last commit
  {
    auto topology = builder.create_topology();
    auto streams = topology->create_processors<kspp::kafka_source<std::string, std::string, kspp::binary_serdes>>(
        partition_list, "kspp_test5");
    auto pipe = topology->create_processor<kspp::pipe<std::string, std::string>>(-1);
    auto table_stream = topology->create_sink<kspp::kafka_sink<std::string, std::string, kspp::binary_serdes>>("kspp_test5");
    pipe->add_sink(table_stream);

    topology->init(); // remove
    topology->start();

    // insert testdata in pipe
    for (auto &i : test_data) {
      pipe->produce(i.key, i.value);
    }

    std::chrono::time_point<std::chrono::system_clock> end = std::chrono::system_clock::now() + 2000ms;
    while (std::chrono::system_clock::now() < end) {
      topology->flush();
    }
    topology->commit(true);

    {
      auto actual = table_stream->get_metric("in_count");
      assert(actual == TEST_SIZE);
    }

    int64_t sz = 0;
    for (auto &&i : streams)
      sz += i->get_metric("in_count");
    LOG(INFO) << "sz: " << sz << " expected : " <<  TEST_SIZE;

    assert(sz == TEST_SIZE);
  }

  return 0;
}