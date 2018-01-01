#include <iostream>
#include <chrono>
#include <cassert>
#include <kspp/impl/serdes/binary_serdes.h>
#include <kspp/utils/kafka_utils.h>
#include <kspp/topology_builder.h>
#include <kspp/sinks/kafka_sink.h>
#include <kspp/sources/pipe.h>
#include <kspp/state_stores/mem_store.h>
#include <kspp/sources/kafka_source.h>
#include <kspp/sinks/generic_sink.h>

using namespace std::chrono_literals;
using namespace kspp;

struct record {
  std::string key;
  std::string value;
};

#define TEST_SIZE 1000000

int main(int argc, char **argv) {
  FLAGS_logtostderr = 1;
  google::InitGoogleLogging(argv[0]);


  std::vector<record> test_data;
  for (int i = 0; i != TEST_SIZE; ++i) {
    record r;
    r.key = "key" + std::to_string(i);
    r.value = "value" + std::to_string(i);
    test_data.push_back(r);
  }

  auto config = std::make_shared<cluster_config>();
  //broker defaults to env KSPP_KAFKA_BROKER_URL if defined of localhost if not
  config->load_config_from_env();
  config->set_producer_buffering_time(10ms);
  config->set_consumer_buffering_time(10ms);
  config->log(); // optional
  config->validate();// optional

  auto builder = topology_builder("kspp", "test5", config);

  auto nr_of_partitions = kafka::get_number_partitions(config, "kspp_test5");
  auto partition_list = get_partition_list(nr_of_partitions);

  auto t0 = milliseconds_since_epoch();
  {
    auto topology = builder.create_topology();

    // now create new data
    //auto pipe = topology->create_processor<kspp::pipe<std::string, std::string>>(-1);
    auto kafka_sink0 = topology->create_sink<kafka_sink<std::string, std::string, binary_serdes, binary_serdes>>("kspp_test5");
    //pipe->add_sink(table_stream);

    // we need to consume the source to be able to commit - a null sink is perfect
    auto kafka_sources = topology->create_processors<kafka_source<std::string, std::string, binary_serdes, binary_serdes>>(
        partition_list, "kspp_test5");
    auto dummy_sink = topology->create_sink<genric_topic_sink<std::string, std::string>>(kafka_sources, [](auto r){
      /* noop */ });

    topology->start(kspp::OFFSET_END);

    // start running stuff - overtwise it's a race between consumers starting at end and producers starting writing..
    std::chrono::time_point<std::chrono::system_clock> end0 = std::chrono::system_clock::now() + 2s;
    while (std::chrono::system_clock::now() < end0) {
      topology->process(milliseconds_since_epoch());
    }

    // insert testdata in pipe
    for (auto &i : test_data) {
      kafka_sink0->push_back(i.key, i.value, t0);
    }
    kafka_sink0->flush();

    std::chrono::time_point<std::chrono::system_clock> end1 = std::chrono::system_clock::now() + 2s;
    while (std::chrono::system_clock::now() < end1) {
      topology->process(milliseconds_since_epoch());
    }

    std::chrono::time_point<std::chrono::system_clock> end2 = std::chrono::system_clock::now() + 2s;
    while (std::chrono::system_clock::now() < end2) {
      topology->flush();
    }

    topology->commit(true);
    auto written_sz = kafka_sink0->get_metric("in_count");
    assert(written_sz == TEST_SIZE);
    LOG(INFO) << "produced sz:" << written_sz;

    int64_t consumed_sz = 0;
    for (auto &&i : kafka_sources)
      consumed_sz += i->get_metric("in_count");
    LOG(INFO) << "consumed_sz: " << consumed_sz << " expected : " <<  TEST_SIZE;
    assert(consumed_sz == TEST_SIZE);
  }

  // now pick up from last commit
  {
    auto topology = builder.create_topology();
    auto streams = topology->create_processors<kspp::kafka_source<std::string, std::string, kspp::binary_serdes, kspp::binary_serdes>>(
        partition_list, "kspp_test5");
    auto pipe = topology->create_processor<kspp::pipe<std::string, std::string>>(-1);
    auto table_stream = topology->create_sink<kspp::kafka_sink<std::string, std::string, kspp::binary_serdes, kspp::binary_serdes>>("kspp_test5");
    pipe->add_sink(table_stream);
    topology->start(kspp::OFFSET_STORED);

    // insert testdata in pipe
    for (auto &i : test_data) {
      pipe->push_back(i.key, i.value);
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
    for (auto &&i : streams) {
      auto s = i->get_metric("in_count");
      LOG(INFO) << i->simple_name() <<  ", count:" << s;
      sz += i->get_metric("in_count");
    }
    LOG(INFO) << "sz: " << sz << " expected : " <<  TEST_SIZE;

    assert(sz == TEST_SIZE);
  }


  // now pick up from first commit but skip messages older than now()
  {
    auto topology = builder.create_topology();
    auto streams = topology->create_processors<kspp::kafka_source<std::string, std::string, kspp::binary_serdes, kspp::binary_serdes>>(
        partition_list, "kspp_test5", std::chrono::system_clock::now());
    auto pipe = topology->create_processor<kspp::pipe<std::string, std::string>>(-1);
    auto table_stream = topology->create_sink<kspp::kafka_sink<std::string, std::string, kspp::binary_serdes, kspp::binary_serdes>>("kspp_test5");
    pipe->add_sink(table_stream);
    topology->start(kspp::OFFSET_BEGINNING);

    // insert testdata in pipe
    for (auto &i : test_data) {
      pipe->push_back(i.key, i.value);
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
    for (auto &&i : streams) {
      auto s = i->get_metric("in_count");
      LOG(INFO) << i->simple_name() <<  ", count:" << s;
      sz += i->get_metric("in_count");
    }
    LOG(INFO) << "sz: " << sz << " expected : " <<  TEST_SIZE;

    assert(sz == TEST_SIZE);
  }



  return 0;
}