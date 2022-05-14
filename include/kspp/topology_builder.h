#include <cstdlib>
#include <glog/logging.h>
#include <kspp/cluster_config.h>
#include <kspp/topology.h>
#include <kspp/kspp.h>

#pragma once

namespace kspp {
  class topology_builder {
  public:
    topology_builder(std::shared_ptr<kspp::cluster_config> cluster_config)
        : cluster_config_(cluster_config) {
    }

    std::shared_ptr<kspp::topology> create_topology() {
      return std::make_shared<kspp::topology>(cluster_config_, std::to_string(next_topology_id_++));
    }

    std::shared_ptr<kspp::topology> create_internal_topology() {
      return std::make_shared<kspp::topology>(cluster_config_, std::to_string(next_topology_id_++), true);
    }

  private:
    std::shared_ptr<cluster_config> cluster_config_;
    size_t next_topology_id_=0;
  };
}
