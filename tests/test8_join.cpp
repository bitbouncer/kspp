#include <iostream>
#include <cassert>
#include <kspp/kspp.h>
#include <kspp/utils/kafka_utils.h>
#include <kspp/topology_builder.h>
#include <kspp/processors/generic_stream.h>
#include <kspp/state_stores/mem_store.h>
#include <kspp/processors/ktable.h>
#include <kspp/processors/join.h>
#include <kspp/sinks/generic_sink.h>
/*
 * https://cwiki.apache.org/confluence/display/KAFKA/Kafka+Streams+Join+Semantics
 *
 */

//STREAM_1:   1:null, 3:A, 5:B 7:null, 9:C, 12:null, 15:D
//STREAM_2:   2:null, 4:a, 6:b, 8:null, 10:c, 11:null, 13:null, 14:d

using namespace kspp;

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
  auto config = std::make_shared<kspp::cluster_config>();
  //config->load_config_from_env();
  //config->validate();// optional
  //config->log(); // optional

  //KStream-KTable LEFT Join
  {
    auto partition_list = {0};
    auto builder = kspp::topology_builder("kspp-examples", argv[0], config);
    auto topology = builder.create_topology();

    auto streamA = topology->create_processor<kspp::generic_stream<int32_t, std::string>>(0);

    auto streamB = topology->create_processor<kspp::generic_stream<int32_t, std::string>>(0);
    auto ktableB = topology->create_processor<kspp::ktable<int32_t, std::string, kspp::mem_store>>(streamB);

    auto left_join = topology->create_processor<kspp::kstream_left_join<int32_t, std::string, std::string>>(streamA, ktableB);

    std::vector<std::shared_ptr<kspp::krecord<int32_t, std::pair<std::string, boost::optional<std::string>>>>> expected;
    std::vector<std::shared_ptr<kspp::krecord<int32_t, std::pair<std::string, boost::optional<std::string>>>>> actual;
    expected.push_back(kspp::make_left_join_record<int32_t, std::string, std::string>(42, "A", nullptr, 3));
    expected.push_back(make_left_join_record<int32_t, std::string, std::string>(42, "B", "a", 5));
    expected.push_back(make_left_join_record<int32_t, std::string, std::string>(42, "C", nullptr, 9));
    expected.push_back(make_left_join_record<int32_t, std::string, std::string>(42, "D", "d", 15));

    auto sink = topology->create_sink<kspp::genric_topic_sink<int32_t, kspp::left_join<std::string, std::string>::value_type>>(
        left_join,
        [&](auto r) {
          if (r->value()->second)
            actual.push_back(make_left_join_record<int32_t, std::string, std::string>(r->key(), r->value()->first, *r->value()->second, r->event_time()));
          else
            actual.push_back(make_left_join_record<int32_t, std::string, std::string>(r->key(), r->value()->first, nullptr, r->event_time()));
          //std::cerr << r->event_time() << std::endl;
        });
    produce_stream1(*streamA);
    produce_stream2(*streamB);

    topology->start(kspp::OFFSET_BEGINNING);

    for (int64_t ts=0; ts!=20; ++ts)
      topology->process(ts);


    assert(expected.size() == actual.size());
    for (int i = 0; i != expected.size(); ++i) {
      assert(expected[i]->event_time() == actual[i]->event_time());
      assert(expected[i]->key() == actual[i]->key());
      assert(expected[i]->value()->first == actual[i]->value()->first);

      if (!expected[i]->value()->second)
        assert (!actual[i]->value()->second);
      else
        assert (*expected[i]->value()->second == *actual[i]->value()->second);
    }
  }

//KStream-KTable INNER Join
  {
    auto partition_list = {0};
    auto builder = kspp::topology_builder("kspp-examples", argv[0], config);
    auto topology = builder.create_topology();

    auto streamA = topology->create_processor<kspp::generic_stream<int32_t, std::string>>(0);

    auto streamB = topology->create_processor<kspp::generic_stream<int32_t, std::string>>(0);
    auto ktableB = topology->create_processor<kspp::ktable<int32_t, std::string, kspp::mem_store>>(streamB);

    auto left_join = topology->create_processor<kspp::kstream_inner_join<int32_t, std::string, std::string>>(streamA, ktableB);

    std::vector<std::shared_ptr<kspp::krecord<int32_t, std::pair<std::string, std::string>>>> expected;
    std::vector<std::shared_ptr<kspp::krecord<int32_t, std::pair<std::string, std::string>>>> actual;
    expected.push_back(make_inner_join_record<int32_t, std::string, std::string>(42, "B", "a", 5));
    expected.push_back(make_inner_join_record<int32_t, std::string, std::string>(42, "D", "d", 15));

    auto sink = topology->create_sink<kspp::genric_topic_sink<int32_t, kspp::inner_join<std::string, std::string>::value_type>>(
        left_join,
        [&](auto r) {
          actual.push_back(make_inner_join_record<int32_t, std::string, std::string>(r->key(), r->value()->first, r->value()->second, r->event_time()));
          std::cerr << r->event_time() << std::endl;
        });
    produce_stream1(*streamA);
    produce_stream2(*streamB);

    topology->start(kspp::OFFSET_BEGINNING);

    for (int64_t ts=0; ts!=20; ++ts)
      topology->process(ts);


    assert(expected.size() == actual.size());
    for (int i = 0; i != expected.size(); ++i) {
      assert(expected[i]->event_time() == actual[i]->event_time());
      assert(expected[i]->key() == actual[i]->key());
      assert(expected[i]->value()->first == actual[i]->value()->first);
      assert (expected[i]->value()->second == actual[i]->value()->second);
      //assert(expected[i]->value()->second == actual[i]->value()->second);
      //assert(expected[i]->value() == actual[i]->value());
    }
  }

  //KTable-KTable LEFT Join - OLD SEMANTICS (IE MORE null values)
  {
    auto partition_list = {0};
    auto builder = kspp::topology_builder("kspp-examples", argv[0], config);
    auto topology = builder.create_topology();

    auto streamA = topology->create_processor<kspp::generic_stream<int32_t, std::string>>(0);
    auto ktableA = topology->create_processor<kspp::ktable<int32_t, std::string, kspp::mem_store>>(streamA);

    auto streamB = topology->create_processor<kspp::generic_stream<int32_t, std::string>>(0);
    auto ktableB = topology->create_processor<kspp::ktable<int32_t, std::string, kspp::mem_store>>(streamB);

    auto left_join = topology->create_processor<kspp::ktable_left_join<int32_t, std::string, std::string>>(ktableA, ktableB);

    std::vector<std::shared_ptr<kspp::krecord<int32_t, std::pair<std::string, boost::optional<std::string>>>>> expected;
    std::vector<std::shared_ptr<kspp::krecord<int32_t, std::pair<std::string, boost::optional<std::string>>>>> actual;
    expected.push_back(make_left_join_record<int32_t, std::string, std::string>(42, nullptr, 1)); // this is not according to spec - but according to impl...
    expected.push_back(make_left_join_record<int32_t, std::string, std::string>(42, nullptr, 2)); // this is not according to spec - but according to impl...
    expected.push_back(make_left_join_record<int32_t, std::string, std::string>(42, "A", nullptr, 3));
    expected.push_back(make_left_join_record<int32_t, std::string, std::string>(42, "A", "a", 4));
    expected.push_back(make_left_join_record<int32_t, std::string, std::string>(42, "B", "a", 5));
    expected.push_back(make_left_join_record<int32_t, std::string, std::string>(42, "B", "b", 6));
    expected.push_back(make_left_join_record<int32_t, std::string, std::string>(42, nullptr, 7));
    expected.push_back(make_left_join_record<int32_t, std::string, std::string>(42, nullptr, 8));// this is not according to spec - but according to impl...
    expected.push_back(make_left_join_record<int32_t, std::string, std::string>(42, "C", nullptr, 9));
    expected.push_back(make_left_join_record<int32_t, std::string, std::string>(42, "C", "c", 10));
    expected.push_back(make_left_join_record<int32_t, std::string, std::string>(42, "C", nullptr, 11));
    expected.push_back(make_left_join_record<int32_t, std::string, std::string>(42, nullptr, 12));
    expected.push_back(make_left_join_record<int32_t, std::string, std::string>(42, nullptr, 13));// this is not according to spec - but according to impl...
    expected.push_back(make_left_join_record<int32_t, std::string, std::string>(42, nullptr, 14));// this is not according to spec - but according to impl...
    expected.push_back(make_left_join_record<int32_t, std::string, std::string>(42, "D", "d", 15));

    auto sink = topology->create_sink<kspp::genric_topic_sink<int32_t, kspp::left_join<std::string, std::string>::value_type>>(
        left_join,
        [&](auto r) {
          if (r->value()==nullptr)
            actual.push_back(make_left_join_record<int32_t, std::string, std::string>(r->key(), nullptr, r->event_time()));
          else if (r->value()->second)
            actual.push_back(make_left_join_record<int32_t, std::string, std::string>(r->key(), r->value()->first, *r->value()->second, r->event_time()));
          else
            actual.push_back(make_left_join_record<int32_t, std::string, std::string>(r->key(), r->value()->first, nullptr, r->event_time()));
          std::cerr << r->event_time() << std::endl;
        });
    produce_stream1(*streamA);
    produce_stream2(*streamB);

    topology->start(kspp::OFFSET_BEGINNING);

    for (int64_t ts=0; ts!=20; ++ts)
      topology->process(ts);


    assert(expected.size() == actual.size());
    for (int i = 0; i != expected.size(); ++i) {
      //std::cerr << "expected:" << expected[i]->event_time() << ", actual:" << actual[i]->event_time() << std::endl;
      assert(expected[i]->event_time() == actual[i]->event_time());
      assert(expected[i]->key() == actual[i]->key());

      if (expected[i]->value()==nullptr) {
        assert(actual[i]->value() == nullptr);
        continue;
      }

      assert(expected[i]->value()->first == actual[i]->value()->first);

      if (!expected[i]->value()->second)
        assert (!actual[i]->value()->second);
      else
        assert (*expected[i]->value()->second == *actual[i]->value()->second);
    }
  }


  //KTable-KTable INNER Join - OLD SEMANTICS (IE MORE null values)
  {
    auto partition_list = {0};
    auto builder = kspp::topology_builder("kspp-examples", argv[0], config);
    auto topology = builder.create_topology();

    auto streamA = topology->create_processor<kspp::generic_stream<int32_t, std::string>>(0);
    auto ktableA = topology->create_processor<kspp::ktable<int32_t, std::string, kspp::mem_store>>(streamA);

    auto streamB = topology->create_processor<kspp::generic_stream<int32_t, std::string>>(0);
    auto ktableB = topology->create_processor<kspp::ktable<int32_t, std::string, kspp::mem_store>>(streamB);

    auto left_join = topology->create_processor<kspp::ktable_inner_join<int32_t, std::string, std::string>>(ktableA, ktableB);

    std::vector<std::shared_ptr<kspp::krecord<int32_t, std::pair<std::string, std::string>>>> expected;
    std::vector<std::shared_ptr<kspp::krecord<int32_t, std::pair<std::string, std::string>>>> actual;
    expected.push_back(make_inner_join_record<int32_t, std::string, std::string>(42, nullptr, 1)); // this is not according to spec - but according to impl...
    expected.push_back(make_inner_join_record<int32_t, std::string, std::string>(42, nullptr, 2)); // this is not according to spec - but according to impl...
    expected.push_back(make_inner_join_record<int32_t, std::string, std::string>(42, nullptr, 3)); // this is not according to spec - but according to impl...
    expected.push_back(make_inner_join_record<int32_t, std::string, std::string>(42, "A", "a", 4));
    expected.push_back(make_inner_join_record<int32_t, std::string, std::string>(42, "B", "a", 5));
    expected.push_back(make_inner_join_record<int32_t, std::string, std::string>(42, "B", "b", 6));
    expected.push_back(make_inner_join_record<int32_t, std::string, std::string>(42, nullptr, 7));
    expected.push_back(make_inner_join_record<int32_t, std::string, std::string>(42, nullptr, 8));// this is not according to spec - but according to impl...
    expected.push_back(make_inner_join_record<int32_t, std::string, std::string>(42, nullptr, 9));
    expected.push_back(make_inner_join_record<int32_t, std::string, std::string>(42, "C", "c", 10));
    expected.push_back(make_inner_join_record<int32_t, std::string, std::string>(42, nullptr, 11));
    expected.push_back(make_inner_join_record<int32_t, std::string, std::string>(42, nullptr, 12));// this is not according to spec - but according to impl...
    expected.push_back(make_inner_join_record<int32_t, std::string, std::string>(42, nullptr, 13));// this is not according to spec - but according to impl...
    expected.push_back(make_inner_join_record<int32_t, std::string, std::string>(42, nullptr, 14));// this is not according to spec - but according to impl...
    expected.push_back(make_inner_join_record<int32_t, std::string, std::string>(42, "D", "d", 15));

    auto sink = topology->create_sink<kspp::genric_topic_sink<int32_t, kspp::inner_join<std::string, std::string>::value_type>>(
        left_join,
        [&](auto r) {
          if (r->value()==nullptr)
            actual.push_back(make_inner_join_record<int32_t, std::string, std::string>(r->key(), nullptr, r->event_time()));
          else
            actual.push_back(make_inner_join_record<int32_t, std::string, std::string>(r->key(), r->value()->first, r->value()->second, r->event_time()));
          std::cerr << r->event_time() << std::endl;
        });
    produce_stream1(*streamA);
    produce_stream2(*streamB);

    topology->start(kspp::OFFSET_BEGINNING);

    for (int64_t ts=0; ts!=20; ++ts)
      topology->process(ts);


    assert(expected.size() == actual.size());
    for (int i = 0; i != expected.size(); ++i) {
      //std::cerr << "expected:" << expected[i]->event_time() << ", actual:" << actual[i]->event_time() << std::endl;
      assert(expected[i]->event_time() == actual[i]->event_time());
      assert(expected[i]->key() == actual[i]->key());

      if (expected[i]->value()==nullptr) {
        assert(actual[i]->value() == nullptr);
        continue;
      }

      assert(expected[i]->value()->first == actual[i]->value()->first);
      assert (expected[i]->value()->second == actual[i]->value()->second);
    }
  }


  //KTable-KTable OUTER Join - OLD SEMANTICS (IE MORE null values)
  {
    auto partition_list = {0};
    auto builder = kspp::topology_builder("kspp-examples", argv[0], config);
    auto topology = builder.create_topology();

    auto streamA = topology->create_processor<kspp::generic_stream<int32_t, std::string>>(0);
    auto ktableA = topology->create_processor<kspp::ktable<int32_t, std::string, kspp::mem_store>>(streamA);

    auto streamB = topology->create_processor<kspp::generic_stream<int32_t, std::string>>(0);
    auto ktableB = topology->create_processor<kspp::ktable<int32_t, std::string, kspp::mem_store>>(streamB);

    auto left_join = topology->create_processor<kspp::ktable_outer_join<int32_t, std::string, std::string>>(ktableA, ktableB);

    std::vector<std::shared_ptr<kspp::krecord<int32_t, std::pair<boost::optional<std::string>, boost::optional<std::string>>>>> expected;
    std::vector<std::shared_ptr<kspp::krecord<int32_t, std::pair<boost::optional<std::string>, boost::optional<std::string>>>>> actual;
    expected.push_back(make_outer_join_record<int32_t, std::string, std::string>(42, nullptr, 1)); // this is not according to spec - but according to impl...
    expected.push_back(make_outer_join_record<int32_t, std::string, std::string>(42, nullptr, 2)); // this is not according to spec - but according to impl...
    expected.push_back(make_outer_join_record<int32_t, std::string, std::string>(42, "A", nullptr, 3));
    expected.push_back(make_outer_join_record<int32_t, std::string, std::string>(42, "A", "a", 4));
    expected.push_back(make_outer_join_record<int32_t, std::string, std::string>(42, "B", "a", 5));
    expected.push_back(make_outer_join_record<int32_t, std::string, std::string>(42, "B", "b", 6));
    expected.push_back(make_outer_join_record<int32_t, std::string, std::string>(42, nullptr, "b", 7));
    expected.push_back(make_outer_join_record<int32_t, std::string, std::string>(42, nullptr, 8));// this is not according to spec - but according to impl...
    expected.push_back(make_outer_join_record<int32_t, std::string, std::string>(42, "C", nullptr, 9));
    expected.push_back(make_outer_join_record<int32_t, std::string, std::string>(42, "C", "c", 10));
    expected.push_back(make_outer_join_record<int32_t, std::string, std::string>(42, "C", nullptr, 11));
    expected.push_back(make_outer_join_record<int32_t, std::string, std::string>(42, nullptr, 12));
    expected.push_back(make_outer_join_record<int32_t, std::string, std::string>(42, nullptr, 13));// this is not according to spec - but according to impl...
    expected.push_back(make_outer_join_record<int32_t, std::string, std::string>(42, nullptr, "d", 14));
    expected.push_back(make_outer_join_record<int32_t, std::string, std::string>(42, "D", "d", 15));

    auto sink = topology->create_sink<kspp::genric_topic_sink<int32_t, kspp::outer_join<std::string, std::string>::value_type>>(
        left_join,
        [&](auto r) {
          if (r->value()==nullptr)
            actual.push_back(make_outer_join_record<int32_t, std::string, std::string>(r->key(), nullptr, r->event_time()));
          else if (r->value()->first && r->value()->second)
            actual.push_back(make_outer_join_record<int32_t, std::string, std::string>(r->key(), *r->value()->first, *r->value()->second, r->event_time()));
          else if (r->value()->first && !r->value()->second)
            actual.push_back(make_outer_join_record<int32_t, std::string, std::string>(r->key(), *r->value()->first, nullptr, r->event_time()));
          else if (!r->value()->first && r->value()->second)
            actual.push_back(make_outer_join_record<int32_t, std::string, std::string>(r->key(), nullptr, *r->value()->second, r->event_time()));
          std::cerr << r->event_time() << std::endl;
        });
    produce_stream1(*streamA);
    produce_stream2(*streamB);

    topology->start(kspp::OFFSET_BEGINNING);

    for (int64_t ts=0; ts!=20; ++ts)
      topology->process(ts);


    assert(expected.size() == actual.size());
    for (int i = 0; i != expected.size(); ++i) {
      //std::cerr << "expected:" << expected[i]->event_time() << ", actual:" << actual[i]->event_time() << std::endl;
      assert(expected[i]->event_time() == actual[i]->event_time());
      assert(expected[i]->key() == actual[i]->key());

      if (expected[i]->value()==nullptr) {
        assert(actual[i]->value() == nullptr);
        continue;
      }

      if (!expected[i]->value()->second)
        assert (!actual[i]->value()->second);
      else
        assert (*expected[i]->value()->second == *actual[i]->value()->second);

      if (!expected[i]->value()->first)
        assert (!actual[i]->value()->first);
      else
        assert (*expected[i]->value()->first == *actual[i]->value()->first);
    }
  }


  return 0;
}
