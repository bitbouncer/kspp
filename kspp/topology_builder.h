#include <kspp/kspp.h>
#include <cstdlib>
#include <boost/filesystem.hpp>
#include <boost/log/trivial.hpp>
#pragma once

namespace kspp {
class partition_topology : public topology_base
{
  public:
  partition_topology(std::shared_ptr<app_info> info, std::string topology_id, int32_t partition, std::string brokers, std::chrono::milliseconds max_buffering, boost::filesystem::path root_path)
    : topology_base(info, topology_id, partition, brokers, max_buffering, root_path) {}

  template<class pp, typename... Args>
  typename std::enable_if<std::is_base_of<kspp::partition_processor, pp>::value, std::shared_ptr<pp>>::type
    create_processor(Args... args) {
    auto p = std::make_shared<pp>(*this, args...);
    _partition_processors.push_back(p);
    return p;
  }

  template<class pp, typename... Args>
  typename std::enable_if<std::is_base_of<kspp::generic_sink, pp>::value, std::shared_ptr<pp>>::type
    create_topic_sink(Args... args) {
    auto p = std::make_shared<pp>(*this, args...);
    _sinks.push_back(p);
    return p;
  }
};

class topic_topology : public topology_base
{
  public:
  topic_topology(std::shared_ptr<app_info> info, std::string topology_id, std::string brokers, std::chrono::milliseconds max_buffering, boost::filesystem::path root_path)
    : topology_base(info, topology_id, -1, brokers, max_buffering, root_path) {}

  // top level factory
  template<class pp, typename... Args>
  typename std::enable_if<std::is_base_of<kspp::partition_processor, pp>::value, std::vector<std::shared_ptr<pp>>>::type
    create_processors(std::vector<int> partition_list, Args... args) {
    std::vector<std::shared_ptr<pp>> result;
    for (auto i : partition_list) {
      auto p = std::make_shared<pp>(*this, i, args...);
      _partition_processors.push_back(p);
      result.push_back(p);
    }
    return result;
  }

  // should this be removed??
  template<class pp, typename... Args>
  typename std::enable_if<std::is_base_of<kspp::partition_processor, pp>::value, std::shared_ptr<pp>>::type
    create_partition_processor(Args... args) {
    auto p = std::make_shared<pp>(*this, args...);
    _partition_processors.push_back(p);
    return p;
  }

  // when you have a vector of partitions - lets create a new processor layer
  template<class pp, class ps, typename... Args>
  typename std::enable_if<std::is_base_of<kspp::partition_processor, pp>::value, std::vector<std::shared_ptr<pp>>>::type
    create_processors(std::vector<std::shared_ptr<ps>> sources, Args... args) {
    std::vector<std::shared_ptr<pp>> result;
    for (auto i : sources) {
      auto p = std::make_shared<pp>(*this, i, args...);
      _partition_processors.push_back(p);
      result.push_back(p);
    }
    return result;
  }
   
  /**
    joins between two arrays
    we could probably have stricter contrainst on the types of v1 and v2
  */
  template<class pp, class sourceT, class leftT, typename... Args>
  typename std::enable_if<std::is_base_of<kspp::partition_processor, pp>::value, std::vector<std::shared_ptr<pp>>>::type
    create_processors(
    std::vector<std::shared_ptr<sourceT>> v1,
    std::vector<std::shared_ptr<leftT>> v2,
    Args... args) {
    std::vector<std::shared_ptr<pp>> result;
    auto i = v1.begin();
    auto j = v2.begin();
    auto end = v1.end();
    for (; i != end; ++i, ++j) {
      auto p = std::make_shared<pp>(*this, *i, *j, args...);
      _partition_processors.push_back(p);
      result.push_back(p);
    }
    return result;
  }

  // this seems to be only sinks???
  template<class pp, typename... Args>
  typename std::enable_if<std::is_base_of<kspp::generic_sink, pp>::value, std::shared_ptr<pp>>::type
    create_sink(Args... args) {
    auto p = std::make_shared<pp>(*this, args...);
    _sinks.push_back(p);
    return p;
  }

  // this seems to be only sinks???
  template<class pp, class source, typename... Args>
  typename std::enable_if<std::is_base_of<kspp::generic_sink, pp>::value, std::shared_ptr<pp>>::type
    create_sink(std::vector<std::shared_ptr<source>> sources, Args... args) {
    auto p = std::make_shared<pp>(*this, args...);
    _sinks.push_back(p);
    for (auto i : sources)
      i->add_sink(p);
    return p;
  }
};

class topology_builder
{
  public:
  static boost::filesystem::path default_directory() {
    if (const char* env_p = std::getenv("KSPP_STATE_DIR")) {
      return boost::filesystem::path(env_p);
    }
    return boost::filesystem::temp_directory_path();
  }

  static std::string default_kafka_broker() {
    if (const char* env_p = std::getenv("KAFKA_BROKER"))
      return std::string(env_p);
    return "localhost";
  }

  topology_builder(std::shared_ptr<app_info> app_info,
                   std::string brokers = default_kafka_broker(),
                   std::chrono::milliseconds max_buffering = std::chrono::milliseconds(1000),
                   boost::filesystem::path root_path = default_directory())
    : _app_info(app_info)
    , _next_topology_id(0)
    , _brokers(brokers)
    , _max_buffering(max_buffering)
    , _root_path(root_path) {
    BOOST_LOG_TRIVIAL(info) << "topology_builder created, " << to_string(*_app_info) << ", kafka_brokers:" << brokers << ", max_buffering:" << max_buffering .count() << "ms,  root_path:" << root_path;
  }

  std::shared_ptr<partition_topology> create_partition_topology(std::string id, int32_t partition) {
    return std::make_shared<partition_topology>(_app_info, id, partition, _brokers, _max_buffering, _root_path);
  }

  std::shared_ptr<partition_topology> create_partition_topology(int32_t partition) {
    std::string id = "topology-" + std::to_string(_next_topology_id);
    _next_topology_id++;
    return std::make_shared<partition_topology>(_app_info, id, partition, _brokers, _max_buffering, _root_path);
  }

  std::shared_ptr<topic_topology> create_topology() {
    std::string id = "topology-" + std::to_string(_next_topology_id);
    _next_topology_id++;
    return std::make_shared<topic_topology>(_app_info, id, _brokers, _max_buffering, _root_path);
  }

  std::string brokers() const {
    return _brokers;
  }

  private:
  std::shared_ptr<app_info> _app_info;
  std::string               _brokers;
  std::chrono::milliseconds _max_buffering;
  boost::filesystem::path   _root_path;
  size_t                    _next_topology_id;
};
};
