#include <string>
#include <chrono>
#include <mutex>
#include <memory>
#include <set>
#include <librdkafka/rdkafka.h>
#include <librdkafka/rdkafkacpp.h>

#pragma once

namespace kspp {
  class cluster_config;

  class cluster_metadata {
  public:
    cluster_metadata(const cluster_config *);

    ~cluster_metadata();

    void validate();

    int32_t get_number_partitions(std::string topic);

    bool consumer_group_exists(std::string consumer_group, std::chrono::seconds timeout) const; // uses rd kafka c api

    bool wait_for_consumer_group(std::string consumer_group, std::chrono::seconds timeout) const; // uses rd kafka c api

    //bool topic_partition_available(std::string topic, int32_t partition, std::chrono::seconds timeout) const;

    bool wait_for_topic_partition(std::string topic, int32_t partition, std::chrono::seconds timeout) const;

    bool wait_for_topic_leaders(std::string, std::chrono::seconds timeout) const;

  private:
    mutable std::mutex mutex_;
    rd_kafka_t *rk_c_handle_;
    std::unique_ptr<RdKafka::Producer> rk_cpp_handle_;

    mutable std::set<std::string> available_topics_cache_;
    mutable std::set<std::string> available_consumer_groups_;
    mutable std::set<std::string> missing_consumer_groups_;
  };
}