#include "impl/kstream_impl.h"
#include "impl/ktable_impl.h"
#include "sinks/kafka_sink.h"
#include "sinks/stream_sink.h"
#include "sources/kafka_source.h"
#include "processors/pipe.h"
#include "processors/join.h"
#include "processors/count.h"
#include "processors/repartition.h"
#include "processors/filter.h"
#include "processors/transform.h"
#include "processors/rate_limiter.h"
#include "codecs/text_codec.h"

#include <cstdlib>
#include <boost/filesystem.hpp>
#include <boost/log/trivial.hpp>



#pragma once

namespace kspp {
  class partition_topology : public topology_base
  {
    public:
      partition_topology(std::string app_id, std::string topology_id, int32_t partition, std::string brokers, boost::filesystem::path root_path)
      : topology_base(app_id, topology_id, partition, brokers, root_path) {
    }

    template<class pp, typename... Args>
    typename std::enable_if<std::is_base_of<kspp::partition_processor, pp>::value, std::shared_ptr<pp>>::type
      create_processor(Args... args) {
      auto p = std::make_shared<pp>(*this, args...);
      _partition_processors.push_back(p);
      return p;
    }

   /* template<class pp, typename... Args>
    typename std::enable_if<std::is_base_of<kspp::partition_processor, pp>::value, std::shared_ptr<pp>>::type
      create7(Args... args) {
      auto p = std::make_shared<pp>(*this, args...);
      _partition_processors.push_back(p);
      return p;
    }*/

    // this seems to be only sinks???
    template<class pp, typename... Args>
    typename std::enable_if<std::is_base_of<kspp::topic_processor, pp>::value, std::shared_ptr<pp>>::type
      create(Args... args) {
      auto p = std::make_shared<pp>(*this, args...);
      _topic_processors.push_back(p);
      return p;
    }
  };


  class topic_topology : public topology_base
  {
    public:
    topic_topology(std::string app_id, std::string topology_id, std::string brokers, boost::filesystem::path root_path)
      : topology_base(app_id, topology_id, -1, brokers, root_path) {
    }
  
    template<class pp, typename... Args>
    typename std::enable_if<std::is_base_of<kspp::partition_processor, pp>::value, std::shared_ptr<pp>>::type
      create(Args... args) {
      auto p = std::make_shared<pp>(*this, args...);
      _partition_processors.push_back(p);
      return p;
    }

    template<class pp, typename... Args>
    typename std::enable_if<std::is_base_of<kspp::partition_processor, pp>::value, std::vector<std::shared_ptr<pp>>>::type
      create_N(size_t count, Args... args) {
      std::vector<std::shared_ptr<pp>> result;
      for (size_t i = 0; i != count; ++i) {
        auto p = std::make_shared<pp>(*this, i, args...);
        _partition_processors.push_back(p);
        result.push_back(p);
      }
      return result;
    }

    // for flat map???
    template<class pp, class ps, typename... Args>
    typename std::enable_if<std::is_base_of<kspp::partition_processor, pp>::value, std::vector<std::shared_ptr<pp>>>::type
      create(std::vector<std::shared_ptr<ps>> sources, Args... args) {
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
      create(
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
    typename std::enable_if<std::is_base_of<kspp::topic_processor, pp>::value, std::shared_ptr<pp>>::type
      create(Args... args) {
      auto p = std::make_shared<pp>(*this, args...);
      _topic_processors.push_back(p);
      return p;
    }
    
   /* template<class K, class V>
    std::vector<std::shared_ptr<kspp::partition_source<K, V>>> create_kafka_sources(std::string topic, size_t nr_of_partitions) {
      std::vector<std::shared_ptr<kspp::partition_source<K, V>>> result;
      for (size_t i = 0; i != nr_of_partitions; ++i) {
        auto p = std::make_shared<kspp::kafka_source<K, V, CODEC>>(_brokers, topic, i, _default_codec);
        result.push_back(p);
        _partition_processors.push_back(p);
      }
      return result;;
    }

    template<class K, class V>
    std::shared_ptr<kspp::partition_sink<K, V>> create_global_kafka_sink(std::string topic) {
      auto p = kspp::kafka_single_partition_sink<K, V, CODEC>::create(_brokers, topic, 0, _default_codec);
      _partition_processors.push_back(p);
      return p;
    }

    template<class K, class V>
    std::shared_ptr<kspp::topic_sink<K, V, CODEC>> create_global_kafka_topic_sink(std::vector<std::shared_ptr<kspp::partition_source<K, V>>>& source, std::string topic) {
      auto p = kspp::kafka_single_partition_sink<K, V, CODEC>::create(_brokers, topic, 0, _default_codec);
      _partition_processors.push_back(p);
      for (auto i : source)
        i->add_sink(p);
      return p;
    }
  
    template<class K, class V>
    std::shared_ptr<kspp::pipe<K, V>> create_global_pipe(std::vector<std::shared_ptr<kspp::partition_source<K, V>>>& sources) {
      auto pipe = std::make_shared<kspp::pipe<K, V>>(0);
      _partition_processors.push_back(pipe);
      for (auto i : sources)
        i->add_sink([pipe](auto e) {
        pipe->produce(e);
      });
      return pipe;
    }*/


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

    topology_builder(std::string app_id, std::string brokers = default_kafka_broker(), boost::filesystem::path root_path = default_directory())
      : _app_id(app_id)
      , _next_topology_id(0)
      , _brokers(brokers)
      , _root_path(root_path) {
      BOOST_LOG_TRIVIAL(info) << "topology_builder created, app_id:" << app_id << ", kafka_brokers:" << brokers << ", root_path:" << root_path;
    }

    std::shared_ptr<partition_topology> create_topology(std::string id, int32_t partition) {
      return std::make_shared<partition_topology>(_app_id, id, partition, _brokers, _root_path);
    }

    std::shared_ptr<partition_topology> create_topology(int32_t partition) {
      std::string id = "topology-" + std::to_string(_next_topology_id);
      _next_topology_id++;
      return std::make_shared<partition_topology>(_app_id, id, partition, _brokers, _root_path);
    }

    std::shared_ptr<topic_topology> create_topic_topology() {
      std::string id = "topology-" + std::to_string(_next_topology_id);
      _next_topology_id++;
      return std::make_shared<topic_topology>(_app_id, id, _brokers, _root_path);
    }

  private:
    std::string             _app_id;
    std::string             _brokers;
    boost::filesystem::path _root_path;
    size_t                  _next_topology_id;
  };
};
