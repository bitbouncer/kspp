#include <kspp/impl/kafka_utils.h>
#include <thread>
#include <boost/log/trivial.hpp>
#include <librdkafka/rdkafka.h>

using namespace std::chrono_literals;

#define LOGPREFIX_ERROR BOOST_LOG_TRIVIAL(error) << BOOST_CURRENT_FUNCTION

namespace kspp {
  namespace kafka {
    int32_t get_number_partitions(std::string brokers, std::string topic) {
      std::string errstr;
      std::unique_ptr<RdKafka::Conf> conf(RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL));
      /*
      * Set configuration properties
      */
      if (conf->set("metadata.broker.list", brokers, errstr) != RdKafka::Conf::CONF_OK) {
        LOGPREFIX_ERROR << ", failed to set metadata.broker.list " << errstr;
        exit(1);
      }

      if (conf->set("api.version.request", "true", errstr) != RdKafka::Conf::CONF_OK) {
        LOGPREFIX_ERROR << ", failed to set api.version.request " << errstr;
        exit(1);
      }

      auto producer = std::unique_ptr<RdKafka::Producer>(RdKafka::Producer::create(conf.get(), errstr));
      if (!producer) {
        LOGPREFIX_ERROR << ", failed to create producer:" << errstr;
        exit(1);
      }

      // really try to make sure the partition exist before we continue
      RdKafka::Metadata *md = NULL;
      auto _rd_topic = std::unique_ptr<RdKafka::Topic>(RdKafka::Topic::create(producer.get(), topic, nullptr, errstr));
      int32_t nr_of_partitions = 0;
      while (nr_of_partitions == 0) {
        auto ec = producer->metadata(false, _rd_topic.get(), &md, 5000);
        if (ec == 0) {
          const RdKafka::Metadata::TopicMetadataVector *v = md->topics();
          for (auto &&i : *v) {
            if (i->topic() == topic)
              nr_of_partitions = (int32_t) i->partitions()->size();
          }
        }
        if (nr_of_partitions == 0) {
          LOGPREFIX_ERROR << ", waiting for topic " << topic << " to be available";
          std::this_thread::sleep_for(1s);
        }
      }
      delete md;
      return nr_of_partitions;
    }

    int wait_for_partition(std::string brokers, std::string topic, int32_t partition) {
      std::string errstr;
      std::unique_ptr<RdKafka::Conf> conf(RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL));
      /*
      * Set configuration properties
      */
      if (conf->set("metadata.broker.list", brokers, errstr) != RdKafka::Conf::CONF_OK) {
        LOGPREFIX_ERROR << ", failed to set metadata.broker.list " << errstr;
        return -1;
      }

      if (conf->set("api.version.request", "true", errstr) != RdKafka::Conf::CONF_OK) {
        LOGPREFIX_ERROR << ", failed to set api.version.request " << errstr;
        return -1;
      }

      auto producer = std::unique_ptr<RdKafka::Producer>(RdKafka::Producer::create(conf.get(), errstr));
      if (!producer) {
        LOGPREFIX_ERROR << ", failed to create producer:" << errstr;
        return -1;
      }

      // make sure the partition exist before we continue
      RdKafka::Metadata *md = NULL;
      auto _rd_topic = std::unique_ptr<RdKafka::Topic>(RdKafka::Topic::create(producer.get(), topic, nullptr, errstr));
      bool is_available = false;
      while (!is_available) {
        auto ec = producer->metadata(false, _rd_topic.get(), &md, 5000);
        if (ec == 0) {
          const RdKafka::Metadata::TopicMetadataVector *v = md->topics();
          for (auto &&i : *v) {
            auto partitions = i->partitions();
            for (auto &&j : *partitions) {
              if ((j->err() == 0) && (j->id() == partition) && (j->leader() >= 0)) {
                is_available = true;
                break;
              }
            }
          }
        }
        if (!is_available) {
          LOGPREFIX_ERROR << ", waiting for partitions leader to be available";
          std::this_thread::sleep_for(1s);
        }
      }
      delete md;
      return 0;
    }

    int wait_for_partition(RdKafka::Handle *handle, std::string topic, int32_t partition) {
      std::string errstr;
      // make sure the partition exist before we continue
      RdKafka::Metadata *md = NULL;
      auto _rd_topic = std::unique_ptr<RdKafka::Topic>(RdKafka::Topic::create(handle, topic, nullptr, errstr));
      bool is_available = false;
      while (!is_available) {
        auto ec = handle->metadata(false, _rd_topic.get(), &md, 5000);
        if (ec == 0) {
          const RdKafka::Metadata::TopicMetadataVector *v = md->topics();
          for (auto &&i : *v) {
            auto partitions = i->partitions();
            for (auto &&j : *partitions) {
              if ((j->err() == 0) && (j->id() == partition) && (j->leader() >= 0)) {
                is_available = true;
                break;
              }
            }
          }
        }
        if (!is_available) {
          LOGPREFIX_ERROR << ", waiting for partitions leader to be available";
          std::this_thread::sleep_for(1s);
        }
      }
      delete md;
      return 0;
    }

    int wait_for_topic(RdKafka::Handle *handle, std::string topic) {
      std::string errstr;
      // really try to make sure the partition exist before we continue
      RdKafka::Metadata *md = NULL;
      auto _rd_topic = std::unique_ptr<RdKafka::Topic>(RdKafka::Topic::create(handle, topic, nullptr, errstr));
      int64_t nr_available = 0;
      int64_t nr_of_partitions = 0;
      while (nr_of_partitions == 0 || nr_available != nr_of_partitions) {
        auto ec = handle->metadata(false, _rd_topic.get(), &md, 5000);
        if (ec == 0) {
          const RdKafka::Metadata::TopicMetadataVector *v = md->topics();
          for (auto &&i : *v) {
            auto partitions = i->partitions();
            nr_of_partitions = partitions->size();
            nr_available = 0;
            for (auto &&j : *partitions) {
              if ((j->err() == 0) && (j->leader() >= 0)) {
                ++nr_available;
              }
            }
          }
        }
        if (nr_of_partitions == 0 || nr_available != nr_of_partitions) {
          LOGPREFIX_ERROR << ", waiting for all partitions leader to be available";
          std::this_thread::sleep_for(1s);
        }
      }
      delete md;
      return 0;
    }

    int wait_for_group(std::string brokers, std::string group_id) {
      char errstr[128];
      rd_kafka_t *rk;
      /* Create Kafka C handle */
      if (!(rk = rd_kafka_new(RD_KAFKA_PRODUCER, nullptr,
                              errstr, sizeof(errstr)))) {
        LOGPREFIX_ERROR << "Failed to create new producer: " << errstr;
        rd_kafka_destroy(rk);
        return -1;
      }

      /* Add brokers */
      if (rd_kafka_brokers_add(rk, brokers.c_str()) == 0) {
        LOGPREFIX_ERROR << "No valid brokers specified";
        rd_kafka_destroy(rk);
        return -1;
      }

      rd_kafka_resp_err_t err = RD_KAFKA_RESP_ERR_NO_ERROR;
      const struct rd_kafka_group_list *grplist;
      int retries = 5;

      /* FIXME: Wait for broker to come up. This should really be abstracted
      *        by librdkafka. */
      do {
        if (err) {
          LOGPREFIX_ERROR << "Retrying group list in 1s, ec: " << rd_kafka_err2str(err);
          std::this_thread::sleep_for(1s);
        }
        err = rd_kafka_list_groups(rk, group_id.c_str(), &grplist, 5000);
      } while ((err == RD_KAFKA_RESP_ERR__TRANSPORT ||
                err == RD_KAFKA_RESP_ERR_GROUP_LOAD_IN_PROGRESS) &&
               retries-- > 0);

      if (err) {
        LOGPREFIX_ERROR << "Failed to retrieve groups, ec: " << rd_kafka_err2str(err);
        rd_kafka_destroy(rk);
        return -1;
      }
      rd_kafka_group_list_destroy(grplist);
      rd_kafka_destroy(rk);
      return 0;
    }

    int group_exists(std::string brokers, std::string group_id) {
      char errstr[128];
      rd_kafka_t *rk;
      /* Create Kafka C handle */
      if (!(rk = rd_kafka_new(RD_KAFKA_PRODUCER, nullptr,
                              errstr, sizeof(errstr)))) {
        LOGPREFIX_ERROR << "Failed to create new producer: " << errstr;
        rd_kafka_destroy(rk);
        return -1;
      }

      /* Add brokers */
      if (rd_kafka_brokers_add(rk, brokers.c_str()) == 0) {
        LOGPREFIX_ERROR << "No valid brokers specified";
        rd_kafka_destroy(rk);
        return -1;
      }

      rd_kafka_resp_err_t err = RD_KAFKA_RESP_ERR_NO_ERROR;
      const struct rd_kafka_group_list *grplist;
      int retries = 50; // 5 sec

      /* FIXME: Wait for broker to come up. This should really be abstracted
      *        by librdkafka. */
      do {
        if (err) {
          //LOGPREFIX_ERROR << "Retrying group list in 1s, ec: " << rd_kafka_err2str(err) << " " << brokers;
          std::this_thread::sleep_for(100ms);
        }
        err = rd_kafka_list_groups(rk, group_id.c_str(), &grplist, 5000);
      } while ((err == RD_KAFKA_RESP_ERR__TRANSPORT ||
                err == RD_KAFKA_RESP_ERR_GROUP_LOAD_IN_PROGRESS) &&
               retries-- > 0);

      if (err) {
        LOGPREFIX_ERROR << "Failed to retrieve groups, ec: " << rd_kafka_err2str(err);
        rd_kafka_destroy(rk);
        return -1;
      }

      bool found = (grplist->group_cnt > 0);
      rd_kafka_group_list_destroy(grplist);
      rd_kafka_destroy(rk);
      return found ? 0 : -1; // 0 if ok
    }

  }//namespace kafka
} // kspp


