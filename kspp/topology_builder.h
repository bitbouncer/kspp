#include <boost/filesystem.hpp>
#include "join.h"
#include "kstream_impl.h"
#include "ktable_impl.h"
#include "kafka_sink.h"
#include "kafka_source.h"
#pragma once

namespace csi {
template<class codec>
class topology_builder
{
  public:
  topology_builder(std::string brokers, std::string storage_path, std::shared_ptr<codec> default_codec) :
    _brokers(brokers),
    _default_codec(default_codec),
    _storage_path(storage_path) {
    boost::filesystem::create_directories(boost::filesystem::path(storage_path));
  }

  template<class K, class streamV, class tableV, class R>
  std::shared_ptr<left_join<K, streamV, tableV, R>> create_left_join(std::string tag, std::string stream_topic, std::string table_topic, int32_t partition, typename csi::left_join<K, streamV, tableV, R>::value_joiner value_joiner) {
    auto stream = std::make_shared<csi::kstream_impl<K, streamV, codec>>(tag, _brokers, stream_topic, partition, _storage_path, _default_codec);
    auto table = std::make_shared<csi::ktable_impl<K, tableV, codec>>(tag, _brokers, table_topic, partition, _storage_path, _default_codec);
    return std::make_shared<csi::left_join<K, streamV, tableV, R>>(stream, table, value_joiner);
  }

  template<class K, class V>
  std::shared_ptr<csi::ksink<K, V>> create_kafka_sink(std::string topic, int32_t partition) {
    return std::make_shared<csi::kafka_sink<K, V, codec>>(_brokers, topic, _default_codec, [partition](const K&) { return partition; });
  }

  template<class K, class V>
  std::shared_ptr<csi::ksink<K, V>> create_kafka_sink(std::string topic, std::function<uint32_t(const K& key)> partitioner) {
    return std::make_shared<csi::kafka_sink<K, V, codec>>(_brokers, topic, _default_codec, partitioner);
  }

  template<class K, class V>
  std::shared_ptr<csi::ksource<K, V>> create_kafka_source(std::string topic, int32_t partition) {
    return std::make_shared<csi::kafka_source<K, V, codec>>(_brokers, topic, partition, _default_codec);
  }

  template<class K, class V>
  std::shared_ptr<csi::kstream<K, V>> create_kstream(std::string tag, std::string topic, int32_t partition) {
    return std::make_shared<csi::kstream_impl<K, V, codec>>(tag, _brokers, topic, partition, _storage_path, _default_codec);
  }

  template<class K, class V>
  std::shared_ptr<csi::ktable<K, V>> create_ktable(std::string tag, std::string topic, int32_t partition) {
    return std::make_shared<csi::ktable_impl<K, V, codec>>(tag, _brokers, topic, partition, _storage_path, _default_codec);
  }

  private:
  std::string             _brokers;
  std::shared_ptr<codec>  _default_codec;
  std::string             _storage_path;
};




};
