#include <memory>
#include <string>
#include <chrono>
#include <librdkafka/rdkafkacpp.h>

#pragma once
namespace kspp {
  class cluster_config;
  namespace kafka {

    int32_t get_number_partitions(std::shared_ptr<cluster_config>, std::string topic);

    //int wait_for_partition(std::shared_ptr<cluster_config>, std::string topic, int32_t partition);

    bool wait_for_consumer_group(std::shared_ptr<cluster_config> config, std::string group_id, std::chrono::seconds timeout);

    //bool group_exists(std::shared_ptr<cluster_config>, std::string group_id);

    void require_topic_leaders(std::shared_ptr<cluster_config> config, std::string topic, std::chrono::seconds timeout);
    void require_topic_leaders(std::shared_ptr<cluster_config> config, std::string topic);
  }
}

