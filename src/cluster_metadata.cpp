#include <kspp/cluster_metadata.h>
#include <thread>
#include <glog/logging.h>
#include <librdkafka/rdkafka.h> // for stuff that only exists in c code (in rdkafka)
#include <kspp/cluster_config.h>
#include <kspp/internal/rd_kafka_utils.h>

using namespace std::chrono_literals;

namespace kspp {

  inline static int64_t milliseconds_since_epoch() {
    return std::chrono::duration_cast<std::chrono::milliseconds>
        (std::chrono::system_clock::now().time_since_epoch()).count();
  }


  cluster_metadata::cluster_metadata(const cluster_config *config) {
    {
      std::string errstr;
      std::unique_ptr<RdKafka::Conf> conf(RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL));

      //
      // Set configuration properties TODO should we not have SSL config like elsewhere??
      //
      try {
        ::set_broker_config(conf.get(), config);
        //::set_config(conf.get(), "api.version.request", "true");
      }
      catch (std::invalid_argument &e) {
        LOG(FATAL) << " bad config: " << e.what();
      }

      rk_handle_ = std::unique_ptr<RdKafka::Producer>(RdKafka::Producer::create(conf.get(), errstr));
      if (!rk_handle_) {
        LOG(FATAL) << ", failed to create producer:" << errstr;
      }
    }

    std::string errstr;
    std::unique_ptr<RdKafka::Conf> conf(RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL));

    bool done = false;
    bool one_reply = false;

    int64_t expires = milliseconds_since_epoch() + config->get_cluster_state_timeout().count() * 1000;

    while (!done) {
      if (expires < milliseconds_since_epoch()) {
        if (!one_reply)
          LOG(FATAL) << "cant get broker metadata";
        return;
      }

      RdKafka::Metadata *md = nullptr;
      auto ec = rk_handle_->metadata(true, nullptr, &md, 5000);
      if (ec == 0) {
        one_reply = true;
        done = true;
        const RdKafka::Metadata::TopicMetadataVector *v = md->topics();
        for (auto &&i: *v) {
          if (i->err() == 0) {
            topic_data td;
            auto partitions = i->partitions();
            td.nr_of_partitions = partitions->size();
            for (auto &&j: *partitions) {
              if ((j->err() == 0) && (j->leader() >= 0)) {
                td.available_parititions.push_back(j->id());
              } else {
                done = false;
              }
            }
            topic_data_.insert(std::pair<std::string, topic_data>(i->topic(), td));
          } else {
            done = false;
          }
        }
      } else {
        LOG(ERROR) << "rdkafka error: " << RdKafka::err2str(ec);
      }

      delete md;
      if (!done) {
        LOG(INFO) << "waiting for broker metadata to be available - sleeping 1s";
        std::this_thread::sleep_for(1s);
      }
    }
  }

  cluster_metadata::~cluster_metadata() {
    close();
  }

  void cluster_metadata::close() {
    rk_handle_.reset(nullptr);
  }

  void cluster_metadata::validate() {
  }

  bool cluster_metadata::consumer_group_exists(std::string consumer_group, std::chrono::seconds timeout) const {
    std::lock_guard<std::mutex> guard(mutex_);

    if (available_consumer_groups_.find(consumer_group) != available_consumer_groups_.end())
      return true;

    if (missing_consumer_groups_.find(consumer_group) != missing_consumer_groups_.end())
      return false;

    //char errstr[128];
    auto expires = milliseconds_since_epoch() + 1000 * timeout.count();
    rd_kafka_resp_err_t err = RD_KAFKA_RESP_ERR_NO_ERROR;
    const struct rd_kafka_group_list *grplist = nullptr;

    /* FIXME: Wait for broker to come up. This should really be abstracted by librdkafka. */
    do {
      if (expires < milliseconds_since_epoch()) {
        missing_consumer_groups_.insert(consumer_group);
        return false;
      }
      if (err) {
        DLOG(ERROR) << "retrying group list in 1s, ec: " << rd_kafka_err2str(err);
        std::this_thread::sleep_for(1s);
      } else if (grplist) {
        // the previous call must have succeded bu returned an empty list -
        // bug in rdkafka when using ssl - we cannot separate this from a non existent group - we must retry...
        LOG_IF(FATAL, grplist->group_cnt != 0) << "group list should be empty";
        rd_kafka_group_list_destroy(grplist);
        LOG(INFO) << "got empty group list - retrying in 1s";
        std::this_thread::sleep_for(1s);
      }

      err = rd_kafka_list_groups(rk_handle_->c_ptr(), consumer_group.c_str(), &grplist, 1000);

      DLOG_IF(INFO, err != 0) << "rd_kafka_list_groups: " << consumer_group.c_str() << ", res: " << err;
      DLOG_IF(INFO, err == 0)
              << "rd_kafka_list_groups: " << consumer_group.c_str() << ", res: OK" << " grplist->group_cnt: "
              << grplist->group_cnt;
    } while (err == RD_KAFKA_RESP_ERR__TRANSPORT || err == RD_KAFKA_RESP_ERR_GROUP_LOAD_IN_PROGRESS);
    //} while ((err == RD_KAFKA_RESP_ERR__TRANSPORT || err == RD_KAFKA_RESP_ERR_GROUP_LOAD_IN_PROGRESS) ||
    // (err == 0 && grplist && grplist->group_cnt == 0));

    if (err) {
      LOG(ERROR) << "failed to retrieve groups, ec: " << rd_kafka_err2str(err);
      return false;
      //throw std::runtime_error(rd_kafka_err2str(err));
    }
    bool found = (grplist->group_cnt > 0);
    rd_kafka_group_list_destroy(grplist);

    if (found)
      available_consumer_groups_.insert(consumer_group);
    return found;
  }


  /*bool cluster_metadata::wait_for_consumer_group(std::string consumer_group, std::chrono::seconds timeout) const
  {
    std::lock_guard<std::mutex> guard(_mutex);
    // right now there is now way to separate ssl groups existing and not yet gotten a reply...
    return consumer_group_exists(consumer_group, timeout);
  }
   */

  bool cluster_metadata::wait_for_topic_leaders(std::string topic, std::chrono::seconds timeout) const {
    std::lock_guard<std::mutex> guard(mutex_);
    auto item = topic_data_.find(topic);
    if (item != topic_data_.end()) {
      return item->second.available();
    }

    auto expires = milliseconds_since_epoch() + 1000 * timeout.count();

    std::string errstr;
    // really try to make sure the partition exist before we continue
    auto _rd_topic = std::unique_ptr<RdKafka::Topic>(RdKafka::Topic::create(rk_handle_.get(), topic, nullptr, errstr));
    int64_t nr_available = 0;
    int64_t nr_of_partitions = 0;

    while (true) {
      if (expires < milliseconds_since_epoch())
        return false;
      RdKafka::Metadata *md = nullptr;
      auto ec = rk_handle_->metadata(false, _rd_topic.get(), &md, 1000);
      if (ec == 0) {
        const RdKafka::Metadata::TopicMetadataVector *v = md->topics();
        for (auto &&i: *v) {
          auto partitions = i->partitions();
          nr_of_partitions = partitions->size();
          nr_available = 0;
          for (auto &&j: *partitions) {
            if ((j->err() == 0) && (j->leader() >= 0)) {
              ++nr_available;
            }
          }
        }
      }
      delete md;

      if (nr_of_partitions > 0 && (nr_available == nr_of_partitions))
        break;

      LOG(ERROR) << "waiting for all partitions leader to be available, " << topic << ", sleeping 1s";
      std::this_thread::sleep_for(1s);
    }
    //available_topics_cache_.insert(topic);
    return true;
  }

  bool
  cluster_metadata::wait_for_topic_partition(std::string topic, int32_t partition, std::chrono::seconds timeout) const {
    LOG_IF(FATAL, partition < 0);
    std::lock_guard<std::mutex> guard(mutex_);

    // if all partitions are availble - per definition this one is also available...
    auto item = topic_data_.find(topic);
    if (item != topic_data_.end()) {
      return item->second.available();
    }

    auto expires = milliseconds_since_epoch() + 1000 * timeout.count();

    std::string errstr;
    // make sure the partition exist before we continue
    auto _rd_topic = std::unique_ptr<RdKafka::Topic>(RdKafka::Topic::create(rk_handle_.get(), topic, nullptr, errstr));
    bool is_available = false;
    while (!is_available) {
      if (expires < milliseconds_since_epoch())
        return false;
      RdKafka::Metadata *md = nullptr;
      auto ec = rk_handle_->metadata(false, _rd_topic.get(), &md, 1000);
      if (ec == 0) {
        const RdKafka::Metadata::TopicMetadataVector *v = md->topics();
        for (auto &&i: *v) {
          auto partitions = i->partitions();
          for (auto &&j: *partitions) {
            if ((j->err() == 0) && (j->id() == partition) && (j->leader() >= 0)) {
              is_available = true;
              break;
            }
          }
        }
      }
      delete md;
      if (!is_available) {
        LOG(ERROR) << ", waiting for partitions leader to be available, " << topic << ":" << partition;
        std::this_thread::sleep_for(1s);
      }
    }
    return true;
  }


//  int32_t get_number_partitions2(const cluster_config* cconfig, std::string topic) {
//    std::string errstr;
//    std::unique_ptr<RdKafka::Conf> conf(RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL));
//    /*
// * Set configuration properties
// */
//    try {
//      ::set_broker_config(conf.get(), cconfig);
//      //::set_config(conf.get(), "api.version.request", "true");
//    }
//    catch (std::invalid_argument &e) {
//      LOG(FATAL) << "get_number_partitions: " << topic << " bad config " << e.what();
//    }
//    auto producer = std::unique_ptr<RdKafka::Producer>(RdKafka::Producer::create(conf.get(), errstr));
//    if (!producer) {
//      LOG(FATAL) << ", failed to create producer:" << errstr;
//    }
//    // really try to make sure the partition exist before we continue
//
//    auto _rd_topic = std::unique_ptr<RdKafka::Topic>(RdKafka::Topic::create(producer.get(), topic, nullptr, errstr));
//    if (!_rd_topic) {
//      LOG(FATAL) << "failed to create RdKafka::Topic:" << errstr;
//    }
//    int32_t nr_of_partitions = 0;
//    while (nr_of_partitions == 0) {
//      RdKafka::Metadata *md = NULL;
//      auto ec = producer->metadata(false, _rd_topic.get(), &md, 5000);
//      if (ec == 0) {
//        const RdKafka::Metadata::TopicMetadataVector *v = md->topics();
//        for (auto &&i : *v) {
//          if (i->topic() == topic)
//            nr_of_partitions = (int32_t) i->partitions()->size();
//        }
//      }
//      delete md;
//      if (nr_of_partitions == 0) {
//        LOG(ERROR) << "waiting for topic " << topic << " to be available";
//        std::this_thread::sleep_for(1s);
//      }
//    }
//    return nr_of_partitions;
//  }


  uint32_t cluster_metadata::get_number_partitions(std::string topic) {
    std::lock_guard<std::mutex> guard(mutex_);
    auto item = topic_data_.find(topic);
    if (item != topic_data_.end()) {
      return item->second.nr_of_partitions;
    }
    std::string errstr;
    auto _rd_topic = std::unique_ptr<RdKafka::Topic>(RdKafka::Topic::create(rk_handle_.get(), topic, nullptr, errstr));
    if (!_rd_topic) {
      LOG(FATAL) << "failed to create RdKafka::Topic:" << errstr;
    }
    uint32_t nr_of_partitions = 0;
    while (nr_of_partitions == 0) {
      RdKafka::Metadata *md = nullptr;
      auto ec = rk_handle_->metadata(false, _rd_topic.get(), &md, 5000);
      if (ec == 0) {
        const RdKafka::Metadata::TopicMetadataVector *v = md->topics();
        for (auto &&i: *v) {
          if (i->topic() == topic) {
            if (i->err()) {
              LOG(INFO) << "rdkafka metadata() err:" << RdKafka::err2str(i->err());
            } else {
              nr_of_partitions = (int32_t) i->partitions()->size();
            }
          }
        }
      }
      delete md;
      if (nr_of_partitions == 0) {
        LOG(ERROR) << "waiting for topic " << topic << " to be available";
        std::this_thread::sleep_for(1s);
      }
    }

    return nr_of_partitions;
  }
}
