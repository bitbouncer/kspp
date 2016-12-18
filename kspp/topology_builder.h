#include <boost/filesystem.hpp>
#include "processors/join.h"
#include "processors/count.h"
#include "kstream_impl.h"
#include "ktable_impl.h"
#include "kafka_sink.h"
#include "kafka_source.h"
#pragma once

namespace csi {
template<class CODEC>
class topology_builder
{
  public:
  topology_builder(std::string brokers, std::string storage_path, std::shared_ptr<CODEC> default_codec = std::make_shared<CODEC>()) :
    _brokers(brokers),
    _default_codec(default_codec),
    _storage_path(storage_path) {
    boost::filesystem::create_directories(boost::filesystem::path(storage_path));
  }

  template<class K, class streamV, class tableV, class R>
  std::shared_ptr<left_join<K, streamV, tableV, R>> create_left_join(std::string tag, std::string stream_topic, std::string table_topic, int32_t partition, typename csi::left_join<K, streamV, tableV, R>::value_joiner value_joiner) {
    auto stream = std::make_shared<csi::kstream_impl<K, streamV, CODEC>>(tag, _brokers, stream_topic, partition, _storage_path, _default_codec);
    auto table = std::make_shared<csi::ktable_impl<K, tableV, CODEC>>(tag, _brokers, table_topic, partition, _storage_path, _default_codec);
    return std::make_shared<csi::left_join<K, streamV, tableV, R>>(stream, table, value_joiner);
  }

  //TBD we shouyld get rid of void value - we do not require that but how do we tell compiler that????
  template<class K>
  std::shared_ptr<csi::kmaterialized_source<K, size_t>> create_count_keys(std::shared_ptr<ksource<K, void>> source) {
    return std::make_shared<csi::count_keys<K, CODEC>>(source, _storage_path, _default_codec);
  }

  /*
  template<class K>
  std::shared_ptr<csi::count_keys<K, CODEC>> create_count_keys(std::shared_ptr<ksource<K, void>> source) {
    return std::make_shared<csi::count_keys<K, CODEC>>(source, _storage_path, _default_codec);
  }
  */

  /*
  template<class K>
  std::vector<std::shared_ptr<csi::count_keys<K, CODEC>>> create_count_keys(std::vector<std::shared_ptr<ksource<K, void>>>& sources) {
    std::vector<std::shared_ptr<csi::count_keys<K, CODEC>>> res;
    for (auto i : sources)
      res.push_back(create_count_keys(i));
    return res;
  }
  */

  template<class K>
  std::vector<std::shared_ptr<csi::kmaterialized_source<K, size_t>>> create_count_keys(std::vector<std::shared_ptr<ksource<K, void>>>& sources) {
    std::vector<std::shared_ptr<csi::kmaterialized_source<K, size_t>>> res;
    for (auto i : sources)
      res.push_back(create_count_keys(i));
    return res;
  }


  /*
  template<class K, class V, class RK>
  std::shared_ptr<group_by<K, V, RK>> create_group_by(std::string tag, std::string topic, int32_t partition, typename csi::group_by<K, V, RK>::extractor extractor) {
    auto stream = std::make_shared<csi::kafka_source<K, V, codec>>(_brokers, topic, partition, _default_codec);
    return std::make_shared<csi::group_by<K, V, RK>>(stream, value_joiner);
  }
  */

  template<class K, class V>
  std::shared_ptr<csi::ksink<K, V>> create_kafka_sink(std::string topic, int32_t partition) {
    return std::make_shared<csi::kafka_sink<K, V, CODEC>>(_brokers, topic, partition, _default_codec);
  }

  template<class K, class V>
  std::shared_ptr<csi::ksink<K, V>> create_kafka_sink(std::string topic, std::function<uint32_t(const K& key)> partitioner) {
    return std::make_shared<csi::kafka_sink<K, V, CODEC>>(_brokers, topic, partitioner, _default_codec);
  }
    
  template<class K, class V>
  std::shared_ptr<csi::ksource<K, V>> create_kafka_source(std::string topic, int32_t partition) {
    return std::make_shared<csi::kafka_source<K, V, CODEC>>(_brokers, topic, partition, _default_codec);
  }

  template<class K, class V>
  std::vector<std::shared_ptr<csi::ksource<K, V>>> create_kafka_sources(std::string topic, int32_t nr_of_partitions) {
    std::vector<std::shared_ptr<csi::ksource<K, V>>> v;
    for (int i = 0; i != nr_of_partitions; ++i)
      v.push_back(create_kafka_source<K, V>(topic, i));
    return v;
  }

  template<class K, class V>
  std::shared_ptr<csi::kstream<K, V>> create_kstream(std::string tag, std::string topic, int32_t partition) {
    return std::make_shared<csi::kstream_impl<K, V, CODEC>>(tag, _brokers, topic, partition, _storage_path, _default_codec);
  }

  template<class K, class V>
  std::shared_ptr<csi::ktable<K, V>> create_ktable(std::string tag, std::string topic, int32_t partition) {
    return std::make_shared<csi::ktable_impl<K, V, CODEC>>(tag, _brokers, topic, partition, _storage_path, _default_codec);
  }

  private:
  std::string             _brokers;
  std::shared_ptr<CODEC>  _default_codec;
  std::string             _storage_path;
};




};
