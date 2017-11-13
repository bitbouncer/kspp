#include <kspp/utils/kafka_utils.h>
#include <thread>
#include <glog/logging.h>
#include <librdkafka/rdkafka.h>
#include <kspp/impl/rd_kafka_utils.h>
#include <kspp/cluster_metadata.h>

using namespace std::chrono_literals;

namespace kspp {
  namespace kafka {

    int32_t get_number_partitions(std::shared_ptr<cluster_config> config, std::string topic)
    {
      return config->get_cluster_metadata()->get_number_partitions(topic);
    }

    bool wait_for_consumer_group(std::shared_ptr<cluster_config> config, std::string group_id, std::chrono::seconds timeout) {
        return config->get_cluster_metadata()->wait_for_consumer_group(group_id, timeout);
    }

    void require_topic_leaders(std::shared_ptr<cluster_config> config, std::string topic, std::chrono::seconds timeout)
    {
      if (config->get_cluster_metadata()->wait_for_topic_leaders(topic, timeout))
        LOG(FATAL) << "require_topic_leaders failed, topic:" << topic;
    }

    void require_topic_leaders(std::shared_ptr<cluster_config> config, std::string topic){
      require_topic_leaders(config, topic, config->get_cluster_state_timeout());
    }
  }//namespace kafka
} // kspp


