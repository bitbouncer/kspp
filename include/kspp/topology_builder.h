#include <kspp/kspp.h>
#include <cstdlib>
#include <boost/filesystem.hpp>
#include <glog/logging.h>
#include <kspp/utils.h>
#pragma once

namespace kspp {
  class topology_builder {
  public:
    topology_builder(std::shared_ptr<app_info> app_info,
                     std::string brokers = kspp::utils::default_kafka_broker_uri(),
                     std::chrono::milliseconds max_buffering = std::chrono::milliseconds(1000))
             : _app_info(app_info)
              , _next_topology_id(0)
              , _brokers(brokers)
              , _max_buffering(max_buffering) {
      LOG(INFO) << "topology_builder created, " << to_string(*_app_info) << ", kafka_brokers:" << brokers
                << ", max_buffering:" << max_buffering.count() << "ms";
    }

    std::shared_ptr<topology> create_topology() {
      std::string id = "topology-" + std::to_string(_next_topology_id);
      _next_topology_id++;
      return std::make_shared<topology>(_app_info, id, _brokers, _max_buffering);
    }

    std::string brokers() const {
      return _brokers;
    }

  private:
    std::shared_ptr<app_info> _app_info;
    size_t _next_topology_id;
    std::string _brokers;
    std::chrono::milliseconds _max_buffering;
  };
}
