#include <memory>
#include <string>
#include <chrono>
#include <librdkafka/rdkafkacpp.h>

#pragma once
namespace kspp {
  class cluster_config;
  namespace kafka {
    int32_t get_number_partitions(std::shared_ptr<cluster_config>, std::string topic);

    std::vector<int32_t>
    get_partition_list(std::shared_ptr<cluster_config>, std::string topic, std::string partitions = "[-1]");

    bool
    wait_for_consumer_group(std::shared_ptr<cluster_config> config, std::string group_id, std::chrono::seconds timeout);

    void require_topic_leaders(std::shared_ptr<cluster_config> config, std::string topic, std::chrono::seconds timeout);

    void require_topic_leaders(std::shared_ptr<cluster_config> config, std::string topic);
  }
}

