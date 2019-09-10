#include <kspp/utils/kafka_utils.h>
#include <thread>
#include <glog/logging.h>
#include <librdkafka/rdkafka.h>
#include <kspp/internal/rd_kafka_utils.h>
#include <kspp/cluster_metadata.h>
#include <kspp/utils/kspp_utils.h>

using namespace std::chrono_literals;

namespace kspp {
  namespace kafka {

    int32_t get_number_partitions(std::shared_ptr<cluster_config> config, std::string topic)
    {
      return config->get_cluster_metadata()->get_number_partitions(topic);
    }

    bool wait_for_consumer_group(std::shared_ptr<cluster_config> config, std::string group_id, std::chrono::seconds timeout) {
        if (config->get_cluster_metadata()->consumer_group_exists(group_id, timeout)){
          LOG(INFO) << "wait_for_consumer_group: \"" << group_id << "\" - OK";
          return true;
        } else {
          LOG(ERROR) << "wait_for_consumer_group: \"" << group_id << "\" - FAILED";
          return false;
        }
    }

    void require_topic_leaders(std::shared_ptr<cluster_config> config, std::string topic, std::chrono::seconds timeout)
    {
      if (config->get_cluster_metadata()->wait_for_topic_leaders(topic, timeout))
        LOG(INFO) << "require_topic_leaders:   \"" << topic << "\" - OK";
      else
        LOG(FATAL) << "require_topic_leaders:   \"" << topic << "\" - FAILED";
    }

    void require_topic_leaders(std::shared_ptr<cluster_config> config, std::string topic){
      require_topic_leaders(config, topic, config->get_cluster_state_timeout());
    }

    std::vector<int32_t> get_partition_list(std::shared_ptr<cluster_config> config, std::string topic, std::string partitions){
      auto partition_list = parse_partition_list(partitions);
      if (partition_list.size() == 0 || partition_list[0] == -1){
        auto nr_of_partitions = kspp::kafka::get_number_partitions(config, topic);
        partition_list = kspp::get_partition_list(nr_of_partitions);
      }
      return partition_list;
    }
  }//namespace kafka
} // kspp


