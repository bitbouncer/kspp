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
    topology_builder(std::shared_ptr<kspp::cluster_config> cluster_config)
    : _cluster_config(cluster_config)
        , _next_topology_id(0) {
    }

    std::shared_ptr<kspp::topology> create_topology() {
      return std::make_shared<kspp::topology>(_cluster_config, std::to_string(_next_topology_id++));
    }

  private:
    std::shared_ptr<cluster_config> _cluster_config;
    size_t _next_topology_id;
  };
}
