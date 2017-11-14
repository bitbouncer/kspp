#include <cstdlib>
#include <boost/filesystem.hpp>
#include <glog/logging.h>
#include <kspp/cluster_config.h>
#include <kspp/topology.h>
#include <kspp/kspp.h>
#pragma once

namespace kspp {
  class topology_builder {
  public:
    topology_builder(std::string ns, std::string appname, std::shared_ptr<kspp::cluster_config> cluster_config)
        : _app_info(std::make_shared<app_info>(ns, appname))
        , _cluster_config(cluster_config)
        , _next_topology_id(0) {
    }

    topology_builder(std::string ns, std::string appname, std::string instance_name, std::shared_ptr<kspp::cluster_config> cluster_config)
        : _app_info(std::make_shared<app_info>(ns, appname, instance_name))
        , _cluster_config(cluster_config)
        , _next_topology_id(0) {
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
