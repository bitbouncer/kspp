#include <iostream>
#include <chrono>
#include <kspp/impl/serdes/binary_serdes.h>
#include <kspp/impl/serdes/text_serdes.h>
#include <kspp/topology_builder.h>
#include <kspp/sources/kafka_source.h>
#include <kspp/processors/ktable.h>
#include <kspp/processors/join.h>
#include <kspp/processors/flat_map.h>
#include <kspp/sinks/kafka_sink.h>
#include <kspp/sinks/kafka_partition_sink.h>
#include <kspp/sinks/stream_sink.h>
#include <kspp/state_stores/mem_store.h>
#include <kspp/state_stores/mem_windowed_store.h>
#include <kspp/utils/kafka_utils.h>
#include <kspp/utils/env.h>

#define PARTITION 0
using namespace std::chrono_literals;

struct page_view_data {
  int64_t time;
  int64_t user_id;
  std::string url;
};

struct user_profile_data {
  int64_t last_modified_time;
  int64_t user_id;
  std::string email;
};

struct page_view_decorated {
  int64_t time;
  int64_t user_id;
  std::string url;
  std::string email;
};

std::string to_string(const page_view_data &pd) {
  return std::string("time:")
         + std::to_string(pd.time)
         + ", userid:"
         + std::to_string(pd.user_id)
         + ", url:" + pd.url;
}

std::string to_string(const user_profile_data &pd) {
  return std::string("last_modified_time:")
         + std::to_string(pd.last_modified_time)
         + ", userid:"
         + std::to_string(pd.user_id)
         + ", email:"
         + pd.email;
}

std::string to_string(const page_view_decorated &pd) {
  return std::string("time:")
         + std::to_string(pd.time)
         + ", userid:"
         + std::to_string(pd.user_id)
         + ", url:"
         + pd.url
         + ", email:"
         + pd.email;
}

namespace kspp {
  template<>
  size_t binary_serdes::encode(const page_view_data &src, std::ostream &dst) {
    size_t sz = 0;
    sz += binary_serdes::encode(src.time, dst);
    sz += binary_serdes::encode(src.user_id, dst);
    sz += binary_serdes::encode(src.url, dst);
    return dst.good() ? sz : 0;
  }

  template<>
  size_t binary_serdes::decode(std::istream &src, page_view_data &dst) {
    size_t sz = 0;
    sz += binary_serdes::decode(src, dst.time);
    sz += binary_serdes::decode(src, dst.user_id);
    sz += binary_serdes::decode(src, dst.url);
    return src.good() ? sz : 0;
  }

  template<>
  size_t binary_serdes::encode(const user_profile_data &src, std::ostream &dst) {
    size_t sz = 0;
    sz += binary_serdes::encode(src.last_modified_time, dst);
    sz += binary_serdes::encode(src.user_id, dst);
    sz += binary_serdes::encode(src.email, dst);
    return dst.good() ? sz : 0;
  }

  template<>
  size_t binary_serdes::decode(std::istream &src, user_profile_data &dst) {
    size_t sz = 0;
    sz += binary_serdes::decode(src, dst.last_modified_time);
    sz += binary_serdes::decode(src, dst.user_id);
    sz += binary_serdes::decode(src, dst.email);
    return src.good() ? sz : 0;
  }

  template<>
  size_t binary_serdes::encode(const std::pair<int64_t, int64_t> &src, std::ostream &dst) {
    size_t sz = 0;
    sz += binary_serdes::encode(src.first, dst);
    sz += binary_serdes::encode(src.second, dst);
    return dst.good() ? sz : 0;
  }

  template<>
  size_t binary_serdes::decode(std::istream &src, std::pair<int64_t, int64_t> &dst) {
    size_t sz = 0;
    sz += binary_serdes::decode(src, dst.first);
    sz += binary_serdes::decode(src, dst.second);
    return src.good() ? sz : 0;
  }

  template<>
  size_t binary_serdes::encode(const page_view_decorated &src, std::ostream &dst) {
    size_t sz = 0;
    sz += binary_serdes::encode(src.time, dst);
    sz += binary_serdes::encode(src.user_id, dst);
    sz += binary_serdes::encode(src.url, dst);
    sz += binary_serdes::encode(src.email, dst);
    return dst.good() ? sz : 0;
  }

  template<>
  size_t binary_serdes::decode(std::istream &src, page_view_decorated &dst) {
    size_t sz = 0;
    sz += binary_serdes::decode(src, dst.time);
    sz += binary_serdes::decode(src, dst.user_id);
    sz += binary_serdes::decode(src, dst.url);
    sz += binary_serdes::decode(src, dst.email);
    return src.good() ? sz : 0;
  }

  template<>
  size_t text_serdes::encode(const page_view_data &src, std::ostream &dst) {
    auto s = to_string(src);
    dst << s;
    return s.size();
  }

  template<>
  size_t text_serdes::encode(const user_profile_data &src, std::ostream &dst) {
    auto s = to_string(src);
    dst << s;
    return s.size();
  }

  template<>
  size_t text_serdes::encode(const page_view_decorated &src, std::ostream &dst) {
    auto s = to_string(src);
    dst << s;
    return s.size();
  }
} // namespace kspp

template<class T>
std::string ksource_to_string(const T &ksource) {
  std::string res = "ts: " + std::to_string(ksource.event_time())
                    + ", "
                    + std::to_string(ksource.key())
                    + ", "
                    + (ksource.value() ? to_string(*(ksource.value())) : "NULL");
  return res;
}

int main(int argc, char **argv) {
  FLAGS_logtostderr = 1;
  google::InitGoogleLogging(argv[0]);

  auto config = std::make_shared<kspp::cluster_config>();
  config->load_config_from_env();
  config->validate(); // optional
  config->log();// optional
  auto builder = kspp::topology_builder("kspp-examples", argv[0], config);
  {
    auto topology = builder.create_topology();
    auto sink = topology->create_processor<kspp::kafka_partition_sink<int64_t, page_view_data, kspp::binary_serdes, kspp::binary_serdes>>(
        PARTITION,
        "kspp_PageViews");
    sink->push_back(1, {1440557383335, 1, "/home?user=1"});
    sink->push_back(1, {1440557383335, 1, "/home?user=1"});
    sink->push_back(5, {1440557383345, 5, "/home?user=5"});
    sink->push_back(2, {1440557383456, 2, "/profile?user=2"});
    sink->push_back(1, {1440557385365, 1, "/profile?user=1"});
    sink->push_back(1, {1440557385368, 1, "/profile?user=1"});
  }

  {
    auto topology = builder.create_topology();
    auto sink = topology->create_processor<kspp::kafka_partition_sink<int64_t, user_profile_data, kspp::binary_serdes, kspp::binary_serdes>>(
        PARTITION,
        "kspp_UserProfile");
    sink->push_back(1, {1440557383335, 1, "user1@aol.com"});
    sink->push_back(5, {1440557383345, 5, "user5@gmail.com"});
    sink->push_back(2, {1440557383456, 2, "user2@yahoo.com"});
    sink->push_back(1, {1440557385365, 1, "user1-new-email-addr@comcast.com"});
  }

  {
    auto topology = builder.create_topology();
    auto pageviews = topology->create_processor<kspp::kafka_source<int64_t, page_view_data, kspp::binary_serdes, kspp::binary_serdes>>(
        PARTITION,
        "kspp_PageViews");
    auto userprofiles = topology->create_processor<kspp::kafka_source<int64_t, user_profile_data, kspp::binary_serdes, kspp::binary_serdes>>(
        PARTITION,
        "kspp_UserProfile");
    auto pw_sink = topology->create_processor<kspp::stream_sink<int64_t, page_view_data>>(pageviews, &std::cerr);
    auto up_sink = topology->create_processor<kspp::stream_sink<int64_t, user_profile_data>>(userprofiles, &std::cerr);
    topology->start(kspp::OFFSET_BEGINNING);
    topology->flush();
  }

  {
    auto topology = builder.create_topology();

    auto stream = topology->create_processor<kspp::kafka_source<int64_t, page_view_data, kspp::binary_serdes, kspp::binary_serdes>>(
        PARTITION, "kspp_PageViews");

    auto table_source = topology->create_processor<kspp::kafka_source<int64_t, user_profile_data, kspp::binary_serdes, kspp::binary_serdes>>(
        PARTITION, "kspp_UserProfile");

    auto table = topology->create_processor<kspp::ktable<int64_t, user_profile_data, kspp::mem_store>>(table_source);

    auto join = topology->create_processor<kspp::kstream_left_join<int64_t, page_view_data, user_profile_data>>(stream, table);

    auto decorated_data = topology->create_processor<kspp::flat_map<
        int64_t, kspp::left_join<page_view_data, user_profile_data>::value_type,
        int64_t, page_view_decorated>>(
        join,
        [](const auto record, auto flat_map) {
          auto value = std::make_shared<page_view_decorated>();
          value->user_id = record->key();
          value->email = record->value()->second->email;
          value->time = record->value()->first.time;
          value->url = record->value()->first.url;
          auto r = std::make_shared<kspp::krecord<int64_t, page_view_decorated>>(record->key(), value);
          flat_map->push_back(r);
        });

    auto sink = topology->create_sink<kspp::kafka_partition_sink<int64_t, page_view_decorated, kspp::binary_serdes, kspp::binary_serdes>>(
        decorated_data,
        PARTITION,
        "kspp_PageViewsDecorated");

    topology->init_metrics();
    //join->add_sink(sink);

    topology->start(kspp::OFFSET_BEGINNING);
    topology->flush();
    topology->for_each_metrics([](kspp::metric &m) {
      std::cerr << "metrics: " << m.name() << " : " << m.value() << std::endl;
    });
    topology->commit(true);
  }

  {
    auto topology = builder.create_topology();
    auto table_source = topology->create_processor<kspp::kafka_source<int64_t, user_profile_data, kspp::binary_serdes, kspp::binary_serdes>>(
        PARTITION,
        "kspp_UserProfile");
    auto table1 = topology->create_processor<kspp::ktable<int64_t, user_profile_data, kspp::mem_store>>(table_source);
    auto table2 = topology->create_processor<kspp::ktable<int64_t, user_profile_data, kspp::mem_windowed_store>>(  table_source, 1000ms, 10); // 500ms slots and 10 of them...
    topology->start(kspp::OFFSET_STORED);
    topology->flush();

    std::cerr << "using iterators " << std::endl;
    for (auto it = table1->begin(), end = table1->end(); it != end; ++it)
      std::cerr << "item : " << ksource_to_string(**it) << std::endl;

    std::cerr << "using range iterators " << std::endl;
    for (auto i : *table2)
      std::cerr << "item : " << ksource_to_string(*i) << std::endl;

    //wait a little an see what in there now...
    std::this_thread::sleep_for(10s);
    topology->flush();

    std::cerr << "using range iterators " << std::endl;
    for (auto i : *table1)
      std::cerr << "item : " << ksource_to_string(*i) << std::endl;

    std::cerr << "using range iterators " << std::endl;
    for (auto i : *table2)
      std::cerr << "item : " << ksource_to_string(*i) << std::endl;
  }
  return 0;
}
