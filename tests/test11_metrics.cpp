#include <iostream>
#include <cassert>
#include <kspp/kspp.h>
#include <kspp/utils/kafka_utils.h>
#include <kspp/topology_builder.h>
#include <kspp/processors/generic_stream.h>
#include <kspp/state_stores/mem_store.h>
#include <kspp/processors/ktable.h>
#include <kspp/processors/join.h>
#include <kspp/sinks/array_sink.h>
#include <prometheus/text_serializer.h>
#include <kspp/metrics/prometheus_pushgateway_reporter.h>

/*
 * https://cwiki.apache.org/confluence/display/KAFKA/Kafka+Streams+Join+Semantics
 *
 */

//STREAM_1:   1:null, 3:A, 5:B 7:null, 9:C, 12:null, 15:D
//STREAM_2:   2:null, 4:a, 6:b, 8:null, 10:c, 11:null, 13:null, 14:d

using namespace std::chrono_literals;
using namespace kspp;

template<class KEY, class LEFT, class RIGHT>
std::shared_ptr<kspp::krecord<KEY, std::pair<LEFT, std::optional<RIGHT>>>>
make_left_join_record(KEY key, LEFT a, RIGHT b, int64_t ts) {
  auto pair = std::make_shared<std::pair<LEFT, std::optional<RIGHT>>>(a, b);
  return std::make_shared<kspp::krecord<KEY, std::pair<LEFT, std::optional<RIGHT>>>>(key, pair, ts);
}



void produce_stream1(kspp::event_consumer<int32_t, std::string>& stream) {
  stream.push_back(std::make_shared<kspp::krecord < int32_t, std::string>>(42, nullptr, 1));
  stream.push_back(std::make_shared<kspp::krecord < int32_t, std::string>>(42, "A", 3));
  stream.push_back(std::make_shared<kspp::krecord < int32_t, std::string>>(42, "B", 5));
  stream.push_back(std::make_shared<kspp::krecord < int32_t, std::string>>(42, nullptr, 7));
  stream.push_back(std::make_shared<kspp::krecord < int32_t, std::string>>(42, "C", 9));
  stream.push_back(std::make_shared<kspp::krecord < int32_t, std::string>>(42, nullptr, 12));
  stream.push_back(std::make_shared<kspp::krecord < int32_t, std::string>>(42, "D", 15));
}

void produce_stream2(kspp::event_consumer<int32_t, std::string>& stream) {
  stream.push_back(std::make_shared<kspp::krecord < int32_t, std::string>>(42, nullptr, 2));
  stream.push_back(std::make_shared<kspp::krecord < int32_t, std::string>>(42, "a", 4));
  stream.push_back(std::make_shared<kspp::krecord < int32_t, std::string>>(42, "b", 6));
  stream.push_back(std::make_shared<kspp::krecord < int32_t, std::string>>(42, nullptr, 8));
  stream.push_back(std::make_shared<kspp::krecord < int32_t, std::string>>(42, "c", 10));
  stream.push_back(std::make_shared<kspp::krecord < int32_t, std::string>>(42, nullptr, 11));
  stream.push_back(std::make_shared<kspp::krecord < int32_t, std::string>>(42, nullptr, 13));
  stream.push_back(std::make_shared<kspp::krecord < int32_t, std::string>>(42, "d", 14));
}


int main(int argc, char **argv) {
  std::string consumer_group("kspp-examples");
  auto config = std::make_shared<kspp::cluster_config>(consumer_group, 0);
  //config->load_config_from_env();
  //config->validate();// optional
  //config->log(); // optional

  //KStream-KTable LEFT Join
  {
    auto partition_list = {0};
    kspp::topology_builder builder(config);
    auto topology = builder.create_topology();

    auto streamA = topology->create_processor<kspp::generic_stream<int32_t, std::string>>(0);

    auto streamB = topology->create_processor<kspp::generic_stream<int32_t, std::string>>(0);
    auto ktableB = topology->create_processor<kspp::ktable<int32_t, std::string, kspp::mem_store>>(streamB);

    auto left_join = topology->create_processor<kspp::kstream_left_join<int32_t, std::string, std::string>>(streamA, ktableB);

    std::vector<std::shared_ptr<const kspp::krecord<int32_t, std::pair<std::string, std::optional<std::string>>>>> expected;
    std::vector<std::shared_ptr<const kspp::krecord<int32_t, std::pair<std::string, std::optional<std::string>>>>> actual;
    expected.push_back(make_left_join_record<int32_t, std::string, std::string>(42, "A", nullptr, 3));
    expected.push_back(make_left_join_record<int32_t, std::string, std::string>(42, "B", "a", 5));
    expected.push_back(make_left_join_record<int32_t, std::string, std::string>(42, "C", nullptr, 9));
    expected.push_back(make_left_join_record<int32_t, std::string, std::string>(42, "D", "d", 15));

    auto sink = topology->create_sink<kspp::array_topic_sink<int32_t, kspp::left_join<std::string, std::string>::value_type>>(
        left_join,
        &actual);

    produce_stream1(*streamA);
    produce_stream2(*streamB);

    topology->add_labels( { { "app_name", "test" } });

    topology->start(kspp::OFFSET_BEGINNING);

    for (int64_t ts=0; ts!=20; ++ts)
      topology->process(ts);

    assert(expected.size() == actual.size());
    for (int i = 0; i != expected.size(); ++i)
      assert(*expected[i] == *actual[i]);

    auto r = topology->get_prometheus_registry();
    auto v =  r->Collect();

    auto metrics_reporter = std::make_shared<kspp::prometheus_pushgateway_reporter>("test11", "localhost:9091") << topology;

    prometheus::TextSerializer serializer;
    serializer.Serialize(std::cout, v);

    while(true)
      std::this_thread::sleep_for(100ms);

  }
  return 0;
}
