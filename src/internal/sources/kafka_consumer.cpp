#include <librdkafka/rdkafka.h> // for stuff that only exists in c code (in rdkafka), must be included first
#include <kspp/internal/sources/kafka_consumer.h>
#include <thread>
#include <chrono>
#include <glog/logging.h>
#include <kspp/utils/kafka_utils.h>
#include <kspp/internal/rd_kafka_utils.h>
#include <kspp/kspp.h>
#include <kspp/cluster_metadata.h>


using namespace std::chrono_literals;
namespace kspp {
  void kafka_consumer::MyEventCb::event_cb(RdKafka::Event &event) {
    switch (event.type()) {
      case RdKafka::Event::EVENT_ERROR:
        LOG(ERROR) << RdKafka::err2str(event.err()) << " " << event.str();
        //if (event.err() == RdKafka::ERR__ALL_BROKERS_DOWN) TODO
        //  run = false;
        break;

      case RdKafka::Event::EVENT_STATS:
        LOG(INFO) << "STATS: " << event.str();
        break;

      case RdKafka::Event::EVENT_LOG:
        LOG(INFO) << event.fac() << ", " << event.str();
        break;

      default:
        LOG(INFO) << "EVENT " << event.type() << " (" << RdKafka::err2str(event.err()) << "): " << event.str();
        break;
    }
  }

  kafka_consumer::kafka_consumer(std::shared_ptr<cluster_config> config, std::string topic, int32_t partition,
                                 std::string consumer_group, bool check_cluster)
      : config_(config), topic_(topic), partition_(partition), consumer_group_(consumer_group) {
    // really try to make sure the partition & group exist before we continue

    if (check_cluster) {
      LOG_IF(FATAL, config->get_cluster_metadata()->wait_for_topic_partition(topic, partition_,
                                                                             config->get_cluster_state_timeout()) ==
                    false)
              << "failed to wait for topic leaders, topic:" << topic << ":" << partition_;
      //wait_for_partition(_consumer.get(), _topic, _partition);
      //kspp::kafka::wait_for_group(brokers, consumer_group); something seems to wrong in rdkafka master.... TODO
    }

    topic_partition_.push_back(RdKafka::TopicPartition::create(topic_, partition_));

    /*
     * Create configuration objects
    */
    std::unique_ptr<RdKafka::Conf> conf(RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL));
    /*
    * Set configuration properties
    */
    try {
      set_broker_config(conf.get(), config_.get());

      //set_config(conf.get(), "api.version.request", "true");
      set_config(conf.get(), "socket.nagle.disable", "true");
      set_config(conf.get(), "fetch.wait.max.ms", std::to_string(config_->get_consumer_buffering_time().count()));
      //set_config(conf.get(), "queue.buffering.max.ms", "100");
      //set_config(conf.get(), "socket.blocking.max.ms", "100");
      set_config(conf.get(), "enable.auto.commit", "false");
      set_config(conf.get(), "auto.commit.interval.ms", "5000"); // probably not needed
      set_config(conf.get(), "enable.auto.offset.store", "false");
      set_config(conf.get(), "group.id", consumer_group_);
      set_config(conf.get(), "enable.partition.eof", "true");
      set_config(conf.get(), "log.connection.close", "false");
      set_config(conf.get(), "max.poll.interval.ms", "86400000"); // max poll interval before leaving consumer group
      set_config(conf.get(), "event_cb", &event_cb_);

      //set_config(conf.get(), "socket.max.fails", "1000000");
      //set_config(conf.get(), "message.send.max.retries", "1000000");// probably not needed

      // following are topic configs but they will be passed in default_topic_conf to broker config.
      std::unique_ptr<RdKafka::Conf> tconf(RdKafka::Conf::create(RdKafka::Conf::CONF_TOPIC));
      set_config(tconf.get(), "auto.offset.reset", "earliest");

      set_config(conf.get(), "default_topic_conf", tconf.get());
    }
    catch (std::invalid_argument &e) {
      LOG(FATAL) << "kafka_consumer topic:" << topic_ << ":" << partition_ << ", bad config " << e.what();
    }

    /*
    * Create consumer using accumulated global configuration.
    */
    std::string errstr;
    consumer_ = std::unique_ptr<RdKafka::KafkaConsumer>(RdKafka::KafkaConsumer::create(conf.get(), errstr));
    if (!consumer_) {
      LOG(FATAL) << "kafka_consumer topic:" << topic_ << ":" << partition_ << ", failed to create consumer, reason: "
                 << errstr;
    }
    LOG(INFO) << "kafka_consumer topic:" << topic_ << ":" << partition_ << ", created";
  }

  kafka_consumer::~kafka_consumer() {
    if (!closed_)
      close();
    for (auto i: topic_partition_) // should be exactly 1
      delete i;
    LOG(INFO) << "consumer deleted";
  }

  void kafka_consumer::close() {
    if (closed_)
      return;
    closed_ = true;
    if (consumer_) {
      consumer_->close();
      LOG(INFO) << "kafka_consumer topic:" << topic_ << ":" << partition_ << ", closed - consumed " << msg_cnt_
                << " messages (" << msg_bytes_ << " bytes)";
    }
    consumer_.reset(nullptr);
  }

  void kafka_consumer::start(int64_t offset) {
    if (offset == kspp::OFFSET_STORED) {
      //just make shure we're not in for any surprises since this is a runtime variable in rdkafka...
      assert(kspp::OFFSET_STORED == RdKafka::Topic::OFFSET_STORED);

      if (consumer_group_exists(consumer_group_, 5s)) {
        DLOG(INFO) << "kafka_consumer::start topic:" << topic_ << ":" << partition_ << " consumer group: "
                   << consumer_group_ << " starting from OFFSET_STORED";
      } else {
        //non existing consumer group means start from beginning
        LOG(INFO) << "kafka_consumer::start topic:" << topic_ << ":" << partition_ << " consumer group: "
                  << consumer_group_ << " missing -> starting from OFFSET_BEGINNING";
        offset = kspp::OFFSET_BEGINNING;
      }
    } else if (offset == kspp::OFFSET_BEGINNING) {
      DLOG(INFO) << "kafka_consumer::start topic:" << topic_ << ":" << partition_ << " consumer group: "
                 << consumer_group_ << " starting from OFFSET_BEGINNING";
    } else if (offset == kspp::OFFSET_END) {
      DLOG(INFO) << "kafka_consumer::start topic:" << topic_ << ":" << partition_ << " consumer group: "
                 << consumer_group_ << " starting from OFFSET_END";
    } else {
      DLOG(INFO) << "kafka_consumer::start topic:" << topic_ << ":" << partition_ << " consumer group: "
                 << consumer_group_ << " starting from fixed offset: " << offset;
    }

    /*
    * Subscribe to topics
    */
    topic_partition_[0]->set_offset(offset);
    RdKafka::ErrorCode err0 = consumer_->assign(topic_partition_);
    if (err0) {
      LOG(FATAL) << "kafka_consumer topic:" << topic_ << ":" << partition_ << ", failed to subscribe, reason:"
                 << RdKafka::err2str(err0);
    }

    update_eof();
  }

  void kafka_consumer::stop() {
    if (consumer_) {
      RdKafka::ErrorCode err = consumer_->unassign();
      if (err) {
        LOG(FATAL) << "kafka_consumer::stop topic:"
                   << topic_
                   << ":"
                   << partition_
                   << ", failed to stop, reason:"
                   << RdKafka::err2str(err);
      }
    }
  }

  int kafka_consumer::update_eof() {
    int64_t low = 0;
    int64_t high = 0;
    RdKafka::ErrorCode ec = consumer_->query_watermark_offsets(topic_, partition_, &low, &high, 1000);
    if (ec) {
      LOG(ERROR) << "kafka_consumer topic:" << topic_ << ":" << partition_
                 << ", consumer.query_watermark_offsets failed, reason:" << RdKafka::err2str(ec);
      return ec;
    }

    if (low == high) {
      eof_ = true;
      LOG(INFO) << "kafka_consumer topic:" << topic_ << ":" << partition_ << " [empty], eof at:" << high;
    } else {
      auto ec = consumer_->position(topic_partition_);
      if (ec) {
        LOG(ERROR) << "kafka_consumer topic:" << topic_ << ":" << partition_ << ", consumer.position failed, reason:"
                   << RdKafka::err2str(ec);
        return ec;
      }
      auto cursor = topic_partition_[0]->offset();
      eof_ = (cursor + 1 == high);
      LOG(INFO) << "kafka_consumer topic:" << topic_ << ":" << partition_ << " cursor: " << cursor << ", eof at:"
                << high;
    }
    return 0;
  }

  std::unique_ptr<RdKafka::Message> kafka_consumer::consume(int librdkafka_timeout) {
    if (closed_ || consumer_ == nullptr) {
      LOG(ERROR) << "topic:" << topic_ << ":" << partition_ << ", consume failed: closed()";
      return nullptr; // already closed
    }

    std::unique_ptr<RdKafka::Message> msg(consumer_->consume(librdkafka_timeout));

    switch (msg->err()) {
      case RdKafka::ERR_NO_ERROR:
        eof_ = false;
        msg_cnt_++;
        msg_bytes_ += msg->len() + msg->key_len();
        return msg;

      case RdKafka::ERR__TIMED_OUT:
        break;

      case RdKafka::ERR__PARTITION_EOF:
        eof_ = true;
        break;

      case RdKafka::ERR__UNKNOWN_TOPIC:
      case RdKafka::ERR__UNKNOWN_PARTITION:
        eof_ = true;
        LOG(ERROR) << "kafka_consumer topic:" << topic_ << ":" << partition_ << ", consume failed: " << msg->errstr();
        break;

      default:
        /* Errors */
        eof_ = true;
        LOG(ERROR) << "kafka_consumer topic:" << topic_ << ":" << partition_ << ", consume failed: " << msg->errstr();
    }
    return nullptr;
  }

  // TBD add time based autocommit
  int32_t kafka_consumer::commit(int64_t offset, bool flush) {
    if (offset < 0) // not valid
      return 0;

    // you should actually write offset + 1, since a new consumer will start at offset.
    offset = offset + 1;

    if (offset <= last_committed_) // already done
    {
      return 0;
    }

    if (closed_ || consumer_ == nullptr) {
      LOG(ERROR) << "kafka_consumer topic:" << topic_ << ":" << partition_ << ", consumer group: " << consumer_group_
                 << ", commit on closed consumer, lost " << offset - last_committed_ << " messsages";
      return -1; // already closed
    }

    can_be_committed_ = offset;
    RdKafka::ErrorCode ec = RdKafka::ERR_NO_ERROR;
    if (flush) {
      LOG(INFO) << "kafka_consumer topic:" << topic_ << ":" << partition_ << ", consumer group: " << consumer_group_
                << ", commiting(flush) offset:" << can_be_committed_;
      topic_partition_[0]->set_offset(can_be_committed_);
      ec = consumer_->commitSync(topic_partition_);
      if (ec == RdKafka::ERR_NO_ERROR) {
        last_committed_ = can_be_committed_;
      } else {
        LOG(ERROR) << "kafka_consumer topic:" << topic_ << ":" << partition_ << ", consumer group: " << consumer_group_
                   << ", failed to commit, reason:" << RdKafka::err2str(ec);
      }
    } else if ((int64_t) (last_committed_ + max_pending_commits_) < can_be_committed_) {
      DLOG(INFO) << "kafka_consumer topic:" << topic_ << ":" << partition_ << ", consumer group: " << consumer_group_
                 << ", lazy commit: offset:" << can_be_committed_;
      topic_partition_[0]->set_offset(can_be_committed_);
      ec = consumer_->commitAsync(topic_partition_);
      if (ec == RdKafka::ERR_NO_ERROR) {
        last_committed_ = can_be_committed_; // not done yet but promised to be written on close...
      } else {
        LOG(ERROR) << "kafka_consumer topic:" << topic_ << ":" << partition_ << ", consumer group: " << consumer_group_
                   << ", failed to commit, reason:" << RdKafka::err2str(ec);
      }
    }
    return ec;
  }

  //virtual ErrorCode metadata (bool all_topics, const Topic *only_rkt,  Metadata **metadatap, int timeout_ms) = 0;

  bool kafka_consumer::consumer_group_exists(std::string consumer_group, std::chrono::seconds timeout) const {
    //char errstr[128];
    auto expires = milliseconds_since_epoch() + 1000 * timeout.count();
    rd_kafka_resp_err_t err = RD_KAFKA_RESP_ERR_NO_ERROR;
    const struct rd_kafka_group_list *grplist = nullptr;

    /* FIXME: Wait for broker to come up. This should really be abstracted by librdkafka. */
    do {
      if (expires < milliseconds_since_epoch()) {
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

      err = rd_kafka_list_groups(consumer_->c_ptr(), consumer_group.c_str(), &grplist, 1000);

      DLOG_IF(INFO, err != 0) << "rd_kafka_list_groups: " << consumer_group.c_str() << ", res: " << err;
      DLOG_IF(INFO, err == 0)
              << "rd_kafka_list_groups: " << consumer_group.c_str() << ", res: OK" << " grplist->group_cnt: "
              << grplist->group_cnt;
    } while (err == RD_KAFKA_RESP_ERR__TRANSPORT || err == RD_KAFKA_RESP_ERR_GROUP_LOAD_IN_PROGRESS ||
             err == RD_KAFKA_RESP_ERR__PARTIAL);

    if (err) {
      LOG(ERROR) << "failed to retrieve groups, ec: " << rd_kafka_err2str(err);
      return false;
      //throw std::runtime_error(rd_kafka_err2str(err));
    }
    bool found = (grplist->group_cnt > 0);
    rd_kafka_group_list_destroy(grplist);
    return found;
  }
} // namespace