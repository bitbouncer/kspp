#include <kspp/kspp.h>
#include <cstdlib>
#include <boost/filesystem.hpp>
#include <glog/logging.h>
#include <kspp/processors/merge.h>
#include <kspp/utils.h>
#pragma once

namespace kspp {

  class topology
          : public topology_base {
  public:
    topology(std::shared_ptr<app_info> info, std::string topology_id, std::string brokers,
             std::chrono::milliseconds max_buffering, boost::filesystem::path root_path)
            : topology_base(info, topology_id, brokers, max_buffering, root_path) {}

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

    // should this be removed?? or renames to create_processor... right now only pipe support this (ie merge)

    template<class pp, typename... Args>
    typename std::enable_if<std::is_base_of<kspp::partition_processor, pp>::value, std::shared_ptr<pp>>::type
    create_processor(Args... args) {
      auto p = std::make_shared<pp>(*this, args...);
      _partition_processors.push_back(p);
      return p;
    }

    // vector of partitions to a partition (merge) rename to merge ?... right now only pipe support this (ie merge)
    /*
     * template<class pp, class ps, typename... Args>
    typename std::enable_if<std::is_base_of<kspp::partition_processor, pp>::value, std::shared_ptr<pp>>::type
    merge(std::vector<std::shared_ptr<ps>> sources, Args... args) {

      std::shared_ptr<pp> result = std::make_shared<pp>(*this, sources, args...);
      _partition_processors.push_back(result);
      return result;
    }*/


    // vector of partitions to a partition (merge) rename to merge ?... right now only pipe support this (ie merge)
    /*
     * template<class ps, typename... Args>
    std::shared_ptr<kspp::partition_source<typename ps::key_type, typename ps::value_type>> merge(std::vector<std::shared_ptr<ps>> sources, Args... args) {
      std::vector<ps*> upstream;
      for (auto i : sources)
        upstream.push_back(i.get());
      std::shared_ptr<kspp::partition_source<typename ps::key_type, typename ps::value_type>> result = std::make_shared<kspp::merge<typename ps::key_type, typename ps::value_type>>(*this, upstream, args...);
      _partition_processors.push_back(result);
      return result;
    }
     */


    template<class ps, typename... Args>
    std::shared_ptr<kspp::merge<typename ps::key_type, typename ps::value_type>>
    merge(std::vector<std::shared_ptr<ps>> sources, Args... args) {
          std::shared_ptr<kspp::merge<typename ps::key_type, typename ps::value_type>> result = std::make_shared<kspp::merge<typename ps::key_type, typename ps::value_type>>(*this, sources, args...);
      _partition_processors.push_back(result);
      return result;
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
        _partition_processors.push_back(std::static_pointer_cast<kspp::partition_processor>(p));
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
    /*
     * template<class pp, class source, typename... Args>
    typename std::enable_if<std::is_base_of<kspp::generic_sink, pp>::value, std::shared_ptr<pp>>::type
    create_sink(std::shared_ptr<source> src, Args... args) {
      auto p = std::make_shared<pp>(*this, args...);
      _sinks.push_back(p);
      src->add_sink(p);
      return p;
  };
     */


    // this seems to be only sinks??? create from vector of sources - return one (sounds like merge??? exept merge is also source)
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

  class topology_builder {
  public:
    topology_builder(std::shared_ptr<app_info> app_info,
                     std::string brokers = kspp::utils::default_kafka_broker_uri(),
                     std::chrono::milliseconds max_buffering = std::chrono::milliseconds(1000),
                     boost::filesystem::path root_path = kspp::utils::default_statestore_directory())
            : _app_info(app_info)
              , _next_topology_id(0)
              , _brokers(brokers)
              , _max_buffering(max_buffering)
              , _root_path(root_path) {
      LOG(INFO) << "topology_builder created, " << to_string(*_app_info) << ", kafka_brokers:" << brokers
                << ", max_buffering:" << max_buffering.count() << "ms,  root_path:" << root_path;
    }

    std::shared_ptr<topology> create_topology() {
      std::string id = "topology-" + std::to_string(_next_topology_id);
      _next_topology_id++;
      return std::make_shared<topology>(_app_info, id, _brokers, _max_buffering, _root_path);
    }

    std::string brokers() const {
      return _brokers;
    }

  private:
    std::shared_ptr<app_info> _app_info;
    size_t _next_topology_id;
    std::string _brokers;
    std::chrono::milliseconds _max_buffering;
    boost::filesystem::path _root_path;
  };
};
