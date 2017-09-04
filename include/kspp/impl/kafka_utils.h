#include <memory>
#include <string>
#include <librdkafka/rdkafkacpp.h>

#pragma once
namespace kspp {
  class cluster_config;
  namespace kafka {

    int32_t get_number_partitions(std::shared_ptr<cluster_config>, std::string topic);

    int wait_for_partition(std::shared_ptr<cluster_config>, std::string topic, int32_t partition);

    int wait_for_partition(RdKafka::Handle *handle, std::string topic, int32_t partition);

    int wait_for_topic(RdKafka::Handle *handle, std::string topic);


    int wait_for_group(std::shared_ptr<cluster_config>, std::string group_id);

    bool group_exists2(std::shared_ptr<cluster_config>, std::string group_id);
  }
}

