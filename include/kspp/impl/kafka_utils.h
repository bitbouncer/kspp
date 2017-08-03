#include <string>
#include <librdkafka/rdkafkacpp.h>

#pragma once
namespace kspp {
  namespace kafka {

    int32_t get_number_partitions(std::string brokers, std::string topic);

    int wait_for_partition(std::string brokers, std::string topic, int32_t partition);

    int wait_for_partition(RdKafka::Handle *handle, std::string topic, int32_t partition);

    int wait_for_topic(RdKafka::Handle *handle, std::string topic);

    int wait_for_group(std::string brokers, std::string group_id);

    int group_exists(std::string brokers, std::string group_id);
  }
}