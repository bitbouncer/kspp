#include <string>
#include <chrono>
#include <mutex>
#include <memory>
#include <set>
#include <map>
#include <librdkafka/rdkafkacpp.h>
#pragma once

namespace kspp {
  class cluster_config;

  class cluster_metadata {
  public:
    cluster_metadata(const cluster_config *);

    ~cluster_metadata();

    void close();

    void validate();

    int32_t get_number_partitions(std::string topic);

    bool consumer_group_exists(std::string consumer_group, std::chrono::seconds timeout) const; // uses rd kafka c api

    bool wait_for_topic_partition(std::string topic, int32_t partition, std::chrono::seconds timeout) const;

    bool wait_for_topic_leaders(std::string, std::chrono::seconds timeout) const;

  private:
    struct topic_data
    {
      inline bool available() const { return nr_of_partitions == available_parititions.size(); }
      int32_t nr_of_partitions;
      std::vector<int32_t> available_parititions;
    };

    mutable std::mutex _mutex;
    std::unique_ptr<RdKafka::Producer> _rk_handle;
    mutable std::set<std::string> _available_consumer_groups;
    mutable std::set<std::string> _missing_consumer_groups;
    mutable std::map<std::string, topic_data> _topic_data;
  };
}