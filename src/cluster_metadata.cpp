#include <kspp/cluster_metadata.h>
#include <thread>
#include <glog/logging.h>
#include <kspp/cluster_config.h>
#include <kspp/impl/rd_kafka_utils.h>

using namespace std::chrono_literals;

namespace kspp{

  inline static int64_t milliseconds_since_epoch() {
    return std::chrono::duration_cast<std::chrono::milliseconds>
        (std::chrono::system_clock::now().time_since_epoch()).count();
  }

  // STUFF THATS MISSING IN LIBRDKAFKA C++ API...
  // WE DO THIS THE C WAY INSTEAD
  // NEEDS TO BE REWRITTEN USING USING C++ API

  // copy of c++ code
  static void set_config(rd_kafka_conf_t* rd_conf, std::string key, std::string value) {
    char errstr[128];
    if (rd_kafka_conf_set(rd_conf, key.c_str(), value.c_str(), errstr, sizeof(errstr)) !=  RD_KAFKA_CONF_OK) {
      throw std::invalid_argument("\"" + key + "\" -> " + value + ", error: " + errstr);
    }
    DLOG(INFO) << "rd_kafka set_config: " << key << "->" << value;
  }

  // copy of c++ code
  static void set_broker_config(rd_kafka_conf_t *rd_conf, const cluster_config* config) {
    set_config(rd_conf, "bootstrap.servers", config->get_brokers());

    if (config->get_brokers().substr(0, 3) == "ssl") {
      // SSL no auth - always
      set_config(rd_conf, "security.protocol", "ssl");
      set_config(rd_conf, "ssl.ca.location", config->get_ca_cert_path());

      //client cert
      set_config(rd_conf, "ssl.certificate.location", config->get_client_cert_path());
      set_config(rd_conf, "ssl.key.location", config->get_private_key_path());
      // optional password
      if (config->get_private_key_passphrase().size())
        set_config(rd_conf, "ssl.key.password", config->get_private_key_passphrase());
    }
  }

  cluster_metadata::cluster_metadata(const cluster_config* config)
      : rk_c_handle_(nullptr) {
    auto rd_conf = rd_kafka_conf_new();
    try {
      set_broker_config(rd_conf, config);
      set_config(rd_conf, "api.version.request", "true");
    } catch (std::exception& e) {
      LOG(FATAL) << "could not set rd kafka config : " << e.what();
    }

    {
      char errstr[128];
      rk_c_handle_ = rd_kafka_new(RD_KAFKA_PRODUCER, rd_conf, errstr, sizeof(errstr));
      LOG_IF(FATAL, rk_c_handle_ == nullptr) << "rd_kafka_new failed err: " << errstr;
    }


    {
      std::string errstr;
      std::unique_ptr<RdKafka::Conf> conf(RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL));

      /*
      * Set configuration properties
      */
      try {
        ::set_broker_config(conf.get(), config);
        ::set_config(conf.get(), "api.version.request", "true");
      }
      catch (std::invalid_argument &e) {
        LOG(FATAL) << " bad config: " << e.what();
      }
      auto producer = std::unique_ptr<RdKafka::Producer>(RdKafka::Producer::create(conf.get(), errstr));
      if (!producer) {
        LOG(FATAL) << ", failed to create producer:" << errstr;
      }
      rk_cpp_handle_ = std::unique_ptr<RdKafka::Producer>(RdKafka::Producer::create(conf.get(), errstr));
    }
  }

  cluster_metadata::~cluster_metadata(){
    if (rk_c_handle_)
      rd_kafka_destroy(rk_c_handle_);
  }

  void cluster_metadata::validate()
  {
    // TODO - make a call to kafka - maybe __consumer_offsets
  }

  bool cluster_metadata::consumer_group_exists(std::string consumer_group, std::chrono::seconds timeout) const {
    std::lock_guard<std::mutex> guard(mutex_);

    if (available_consumer_groups_.find(consumer_group)!=available_consumer_groups_.end())
      return true;

    if (missing_consumer_groups_.find(consumer_group)!=missing_consumer_groups_.end())
      return false;

    char errstr[128];
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

        err = rd_kafka_list_groups(rk_c_handle_, consumer_group.c_str(), &grplist, 1000);

      DLOG_IF(INFO, err!=0) << "rd_kafka_list_groups: " << consumer_group.c_str() << ", res: " << err;
      DLOG_IF(INFO, err==0) << "rd_kafka_list_groups: " << consumer_group.c_str() << ", res: OK" << " grplist->group_cnt: "
                            << grplist->group_cnt;
    } while ((err == RD_KAFKA_RESP_ERR__TRANSPORT || err == RD_KAFKA_RESP_ERR_GROUP_LOAD_IN_PROGRESS) ||
        (err == 0 && grplist && grplist->group_cnt == 0));

    if (err) {
      LOG(ERROR) << "failed to retrieve groups, ec: " << rd_kafka_err2str(err);
      return false;
      //throw std::runtime_error(rd_kafka_err2str(err));
    }
    bool found = (grplist->group_cnt > 0);
    rd_kafka_group_list_destroy(grplist);

    available_consumer_groups_.insert(consumer_group);
    return found;
  }


  bool cluster_metadata::wait_for_consumer_group(std::string consumer_group, std::chrono::seconds timeout) const
  {
    // right now there is now way to separate ssl groups existing and not yet gotten a reply...
    return consumer_group_exists(consumer_group, timeout);
  }

  bool cluster_metadata::wait_for_topic_leaders(std::string topic, std::chrono::seconds timeout) const {
    std::lock_guard<std::mutex> guard(mutex_);
    if (available_topics_cache_.find(topic)!=available_topics_cache_.end())
      return true;

    auto expires = milliseconds_since_epoch() + 1000 * timeout.count();

    std::string errstr;
    // really try to make sure the partition exist before we continue
    RdKafka::Metadata *md = NULL;
    auto _rd_topic = std::unique_ptr<RdKafka::Topic>(RdKafka::Topic::create(rk_cpp_handle_.get(), topic, nullptr, errstr));
    int64_t nr_available = 0;
    int64_t nr_of_partitions = 0;
    while (nr_of_partitions == 0 || nr_available != nr_of_partitions) {
      if (expires < milliseconds_since_epoch())
        return false;
      auto ec = rk_cpp_handle_->metadata(false, _rd_topic.get(), &md, 1000);
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
        LOG(ERROR) << "waiting for all partitions leader to be available, " << topic << ", sleeping 1s";
        std::this_thread::sleep_for(1s);
      }
    }

    available_topics_cache_.insert(topic);

    delete md;
    return true;
  }

  bool cluster_metadata::wait_for_topic_partition(std::string topic, int32_t partition, std::chrono::seconds timeout) const {
    LOG_IF(FATAL, partition<0);
    std::lock_guard<std::mutex> guard(mutex_);

    // if all partitions are availble - per definition this one is also available...
    if (available_topics_cache_.find(topic)!=available_topics_cache_.end())
      return true;

    auto expires = milliseconds_since_epoch() + 1000 * timeout.count();

    std::string errstr;
    // make sure the partition exist before we continue
    RdKafka::Metadata *md = NULL;
    auto _rd_topic = std::unique_ptr<RdKafka::Topic>(RdKafka::Topic::create(rk_cpp_handle_.get(), topic, nullptr, errstr));
    bool is_available = false;
    while (!is_available) {
      if (expires < milliseconds_since_epoch())
        return false;
      auto ec = rk_cpp_handle_->metadata(false, _rd_topic.get(), &md, 1000);
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
        LOG(ERROR) << ", waiting for partitions leader to be available, " << topic << ":" << partition;
        std::this_thread::sleep_for(1s);
      }
    }
    delete md;
    return true;
  }

  int32_t cluster_metadata::get_number_partitions(std::string topic) {
    std::lock_guard<std::mutex> guard(mutex_);
    std::string errstr;
    std::unique_ptr<RdKafka::Conf> conf(RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL));

    RdKafka::Metadata *md = NULL;
    auto _rd_topic = std::unique_ptr<RdKafka::Topic>(RdKafka::Topic::create(rk_cpp_handle_.get(), topic, nullptr, errstr));
    if (!_rd_topic) {
      LOG(FATAL) << "failed to create RdKafka::Topic:" << errstr;
    }
    int32_t nr_of_partitions = 0;
    while (nr_of_partitions == 0) {
      auto ec = rk_cpp_handle_->metadata(false, _rd_topic.get(), &md, 5000);
      if (ec == 0) {
        const RdKafka::Metadata::TopicMetadataVector *v = md->topics();
        for (auto &&i : *v) {
          if (i->topic() == topic)
            nr_of_partitions = (int32_t) i->partitions()->size();
        }
      }
      if (nr_of_partitions == 0) {
        LOG(ERROR) << "waiting for topic " << topic << " to be available";
        std::this_thread::sleep_for(1s);
      }
    }
    delete md;
    return nr_of_partitions;
  }

  /*
   * bool cluster_metadata::topic_partition_available(std::string topic, int32_t partition, std::chrono::seconds timeout) const{

  }
   */
}
