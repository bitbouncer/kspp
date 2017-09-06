#include <kspp/kspp.h>
#include <cstdlib>
#include <boost/filesystem.hpp>
#include <glog/logging.h>
#include <kspp/utils/utils.h>
#include <kspp/cluster_config.h>
#pragma once

namespace kspp {
  class topology_builder {
  public:
    topology_builder(std::shared_ptr<app_info> app_info, std::shared_ptr<cluster_config> cluster_config)
        : _app_info(app_info)
        , _cluster_config(cluster_config)
        , _next_topology_id(0) {
      LOG(INFO) << "topology_builder created, " << to_string(*_app_info) << ", kafka_brokers:" << cluster_config->get_brokers();
    }

    std::shared_ptr<topology> create_topology() {
      return std::make_shared<topology>(
          _app_info,
          _cluster_config,
          std::to_string(_next_topology_id++));
    }

  private:
    std::shared_ptr<app_info> _app_info;
    std::shared_ptr<cluster_config> _cluster_config;
    size_t _next_topology_id;
  };
}
