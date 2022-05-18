#include <kspp/kspp.h>
#include <memory>
#include <strstream>
#include <thread>
#include <glog/logging.h>
#include <kspp/topology.h>
#include <kspp/internal/sources/kafka_consumer.h>
#include <kspp/internal/commit_chain.h>

#pragma once

namespace kspp {
  template<class K, class V, class KEY_CODEC, class VAL_CODEC>
  class kafka_source_base : public partition_source<K, V> {
    static constexpr const char *PROCESSOR_NAME = "kafka_source";
  public:
    ~kafka_source_base() override {
      close();
    }

    std::string log_name() const override {
      return PROCESSOR_NAME;
    }

    void start(int64_t offset) override {
      impl_.start(offset);
      started_ = true;
    }

    void close() override {
      if (!exit_) {
        exit_ = true;
        thread_.join();
      }

      if (commit_chain_.last_good_offset() >= 0 && impl_.commited() < commit_chain_.last_good_offset())
        impl_.commit(commit_chain_.last_good_offset(), true);
      impl_.close();
    }

    bool eof() const override {
      return incomming_msg_.size() == 0 && impl_.eof();
    }

    void commit(bool flush) override {
      if (commit_chain_.last_good_offset() >= 0)
        impl_.commit(commit_chain_.last_good_offset(), flush);
    }

    // TBD if we store last offset and end of stream offset we can use this...
    size_t queue_size() const override {
      return incomming_msg_.size();
    }

    int64_t next_event_time() const override {
      return incomming_msg_.next_event_time();
    }

    size_t process(int64_t tick) override {
      if (incomming_msg_.size() == 0)
        return 0;
      size_t processed = 0;
      while (!incomming_msg_.empty()) {
        auto p = incomming_msg_.front();
        if (p == nullptr || p->event_time() > tick)
          return processed;
        incomming_msg_.pop_front();
        this->send_to_sinks(p);
        ++(this->processed_count_);
        ++processed;
        this->lag_.add_event_time(tick, p->event_time());
      }
      return processed;
    }

    std::string topic() const override {
      return impl_.topic();
    }

    std::string precondition_topic() const override {
      return impl_.topic();
    }

  protected:
    kafka_source_base(std::shared_ptr<cluster_config> config,
                      std::string topic,
                      int32_t partition,
                      std::string consumer_group,
                      std::chrono::system_clock::time_point start_point,
                      std::shared_ptr<KEY_CODEC> key_codec,
                      std::shared_ptr<VAL_CODEC> val_codec)
        : partition_source<K, V>(nullptr, partition), impl_(config, topic, partition, consumer_group),
          key_codec_(key_codec), val_codec_(val_codec), commit_chain_(topic, partition), start_point_ms_(
            std::chrono::time_point_cast<std::chrono::milliseconds>(start_point).time_since_epoch().count()),
          parse_errors_("parse_errors", "msg"), commit_chain_size_("commit_chain_size", "msg"),
          thread_(&kafka_source_base::thread_f, this){
      this->add_metric(&commit_chain_size_);
      this->add_metric(&parse_errors_);
      this->add_metrics_label(KSPP_PROCESSOR_TYPE_TAG, PROCESSOR_NAME);
      this->add_metrics_label(KSPP_TOPIC_TAG, topic);
      this->add_metrics_label(KSPP_PARTITION_TAG, std::to_string(partition));
    }

    virtual std::shared_ptr<kevent<K, V>> parse(const std::unique_ptr<RdKafka::Message> &ref) = 0;

    void thread_f() {
      while (!started_)
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
      DLOG(INFO) << "starting thread";

      if (start_point_ms_ > 0) {
        DLOG(INFO) << "spooling phase";
        bool done_skipping = false;
        while (!exit_ && !done_skipping) {
          while (auto p = impl_.consume()) {
            if (p->timestamp().timestamp < start_point_ms_) {
            } else {
              done_skipping = true;
              // we need to sent the first message to the queue
              auto decoded_msg = parse(p);
              if (decoded_msg) {
                incomming_msg_.push_back(decoded_msg);
              } else {
                ++parse_errors_;
              }
            }
          }
          commit_chain_size_.set(commit_chain_.size());
          std::this_thread::sleep_for(std::chrono::milliseconds(10)); // wait for more messages
        }
      }

      DLOG(INFO) << "consumption phase";

      while (!exit_) {
        //auto tick = kspp::milliseconds_since_epoch();
        while (auto p = impl_.consume()) {
          auto decoded_msg = parse(p);
          if (decoded_msg) {
            incomming_msg_.push_back(decoded_msg);
          } else {
            ++parse_errors_;
          }

          // to much work in queue - back off and let the consumers work
          while (incomming_msg_.size() > max_incomming_queue_size_ && !exit_) {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
            commit_chain_size_.set(commit_chain_.size());
          }

          // to much uncomitted - back off and let the consumers work
          //while(_commit_chain.size()>10000 && !_exit)
          //  std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        commit_chain_size_.set(commit_chain_.size());
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
      }
      DLOG(INFO) << "exiting thread";
    }

    size_t max_incomming_queue_size_ = 1000;
    bool started_ = false;
    bool exit_ = false;
    event_queue<K, V> incomming_msg_;
    kafka_consumer impl_;
    std::shared_ptr<KEY_CODEC> key_codec_;
    std::shared_ptr<VAL_CODEC> val_codec_;
    commit_chain commit_chain_;
    int64_t start_point_ms_;
    metric_counter parse_errors_;
    metric_gauge commit_chain_size_;
    std::thread thread_;
    //metric_evaluator _commit_chain_size;
  };

  template<class K, class V, class KEY_CODEC, class VAL_CODEC>
  class kafka_source : public kafka_source_base<K, V, KEY_CODEC, VAL_CODEC> {
  public:
    kafka_source(std::shared_ptr<cluster_config> config,
                 int32_t partition,
                 std::string topic,
                 std::shared_ptr<KEY_CODEC> key_codec = std::make_shared<KEY_CODEC>(),
                 std::shared_ptr<VAL_CODEC> val_codec = std::make_shared<VAL_CODEC>())
        : kafka_source_base<K, V, KEY_CODEC, VAL_CODEC>(
        config,
        topic, partition,
        config->get_consumer_group(),
        std::chrono::system_clock::from_time_t(0),
        key_codec,
        val_codec) {
      this->key_codec_->template register_schema<K>(this->topic() + "-key");
      this->val_codec_->template register_schema<V>(this->topic() + "-value");
    }

    kafka_source(std::shared_ptr<cluster_config> config,
                 int32_t partition,
                 std::string topic,
                 std::chrono::system_clock::time_point start_point,
                 std::shared_ptr<KEY_CODEC> key_codec = std::make_shared<KEY_CODEC>(),
                 std::shared_ptr<VAL_CODEC> val_codec = std::make_shared<VAL_CODEC>())
        : kafka_source_base<K, V, KEY_CODEC, VAL_CODEC>(
        config,
        topic, partition,
        config->get_consumer_group(),
        start_point,
        key_codec,
        val_codec) {
      this->key_codec_->template register_schema<K>(this->topic() + "-key");
      this->val_codec_->template register_schema<V>(this->topic() + "-value");
    }

  protected:
    std::shared_ptr<kevent<K, V>> parse(const std::unique_ptr<RdKafka::Message> &ref) override {
      if (!ref)
        return nullptr;

      int64_t timestamp = (ref->timestamp().timestamp >= 0) ? ref->timestamp().timestamp : milliseconds_since_epoch();

      if (ref->key_len() == 0) {
        LOG_EVERY_N(WARNING, 100)
          << this->log_name()
          << ", skipping item with empty key ("
          << google::COUNTER
          << ")";
        return nullptr;
      }

      K tmp_key;
      {

        size_t consumed = this->key_codec_->decode((const char *) ref->key_pointer(), ref->key_len(), tmp_key);
        if (consumed == 0) {
          LOG_IF(ERROR, ref->key_len() != 0)
                  << this->log_name() << ", decode key failed, actual key sz:" << ref->key_len();
          return nullptr;
        } else if (ref->key_len() - consumed > 1) {// patch for 0 terminated string or not... if text encoding
          LOG_FIRST_N(ERROR, 100) << this->log_name() << ", decode key failed, consumed: " << consumed << ", actual: "
                                  << ref->key_len();
          LOG_EVERY_N(ERROR, 1000) << this->log_name() << ", decode key failed, consumed: " << consumed << ", actual: "
                                   << ref->key_len();
          return nullptr;
        }
      }

      std::shared_ptr<V> tmp_value = nullptr;

      size_t sz = ref->len();
      if (sz) {
        tmp_value = std::make_shared<V>();
        size_t consumed = this->val_codec_->decode((const char *) ref->payload(), sz, *tmp_value);
        if (consumed == 0) {
          LOG(ERROR) << this->log_name() << ", decode value failed, size:" << sz;
          return nullptr;
        } else if (sz - consumed > 1) { // patch for 0 terminated string or not... if text encoding
          LOG_FIRST_N(ERROR, 100) << this->log_name() << ", decode value failed, consumed: " << consumed << ", actual: "
                                  << sz;
          LOG_EVERY_N(ERROR, 1000) << this->log_name() << ", decode value failed, consumed: " << consumed
                                   << ", actual: " << sz;
          return nullptr;
        }
      }
      auto record = std::make_shared<krecord<K, V>>(tmp_key, tmp_value, timestamp);
      return std::make_shared<kevent<K, V>>(record, this->commit_chain_.create(ref->offset()));
    }
  };

  // <void, VALUE>
  template<class V, class VAL_CODEC>
  class kafka_source<void, V, void, VAL_CODEC> : public kafka_source_base<void, V, void, VAL_CODEC> {
  public:
    kafka_source(std::shared_ptr<cluster_config> config,
                 int32_t partition,
                 std::string topic,
                 std::shared_ptr<VAL_CODEC> val_codec = std::make_shared<VAL_CODEC>())
        : kafka_source_base<void, V, void, VAL_CODEC>(
        config,
        topic,
        partition,
        config->get_consumer_group(),
        std::chrono::system_clock::from_time_t(0),
        nullptr,
        val_codec) {
      this->val_codec_->template register_schema<V>(this->topic() + "-value");
    }

    kafka_source(std::shared_ptr<cluster_config> config,
                 int32_t partition,
                 std::string topic,
                 std::chrono::system_clock::time_point start_point,
                 std::shared_ptr<VAL_CODEC> val_codec = std::make_shared<VAL_CODEC>())
        : kafka_source_base<void, V, void, VAL_CODEC>(
        config,
        topic, partition,
        config->get_consumer_group(),
        start_point,
        nullptr,
        val_codec) {
      this->val_codec_->template register_schema<V>(this->topic() + "-value");
    }

  protected:
    std::shared_ptr<kevent<void, V>> parse(const std::unique_ptr<RdKafka::Message> &ref) override {
      if (!ref)
        return nullptr;
      size_t sz = ref->len();
      if (sz) {
        int64_t timestamp = (ref->timestamp().timestamp >= 0) ? ref->timestamp().timestamp : milliseconds_since_epoch();
        std::shared_ptr<V> tmp_value = std::make_shared<V>();
        size_t consumed = this->val_codec_->decode((const char *) ref->payload(), sz, *tmp_value);

        if (consumed == 0) {
          LOG(ERROR) << this->log_name() << ", decode value failed, size:" << sz;
          return nullptr;
        } else if (sz - consumed > 1) { // patch for 0 terminated string or not... if text encoding
          LOG_FIRST_N(ERROR, 100) << this->log_name() << ", decode value failed, consumed: " << consumed << ", actual: "
                                  << sz;
          LOG_EVERY_N(ERROR, 1000) << this->log_name() << ", decode value failed, consumed: " << consumed
                                   << ", actual: " << sz;
          return nullptr;
        }

        auto record = std::make_shared<krecord<void, V>>(tmp_value, timestamp);
        return std::make_shared<kevent<void, V>>(record, this->commit_chain_.create(ref->offset()));
      }
      return nullptr; // just parsed an empty message???
    }
  };

  //<KEY, nullptr>
  template<class K, class KEY_CODEC>
  class kafka_source<K, void, KEY_CODEC, void> : public kafka_source_base<K, void, KEY_CODEC, void> {
  public:
    kafka_source(std::shared_ptr<cluster_config> config,
                 int32_t partition,
                 std::string topic,
                 std::shared_ptr<KEY_CODEC> key_codec = std::make_shared<KEY_CODEC>())
        : kafka_source_base<K, void, KEY_CODEC, void>(
        config,
        topic, partition,
        config->get_consumer_group(),
        std::chrono::system_clock::from_time_t(0),
        key_codec,
        nullptr) {
      this->key_codec_->template register_schema<K>(this->topic() + "-key");
    }

    kafka_source(std::shared_ptr<cluster_config> config,
                 int32_t partition,
                 std::string topic,
                 std::chrono::system_clock::time_point start_point,
                 std::shared_ptr<KEY_CODEC> key_codec = std::make_shared<KEY_CODEC>())
        : kafka_source_base<K, void, KEY_CODEC, void>(
        config,
        topic, partition,
        config->get_consumer_group(),
        start_point,
        key_codec,
        nullptr) {
      this->key_codec_->template register_schema<K>(this->topic() + "-key");
    }

  protected:
    std::shared_ptr<kevent<K, void>> parse(const std::unique_ptr<RdKafka::Message> &ref) override {
      if (!ref || ref->key_len() == 0)
        return nullptr;

      int64_t timestamp = (ref->timestamp().timestamp >= 0) ? ref->timestamp().timestamp : milliseconds_since_epoch();
      K tmp_key;
      size_t consumed = this->key_codec_->decode((const char *) ref->key_pointer(), ref->key_len(), tmp_key);
      if (consumed == 0) {
        LOG(ERROR) << this->log_name() << ", decode key failed, actual key sz:" << ref->key_len();
        return nullptr;
      } else if (consumed != ref->key_len()) {
        LOG(ERROR) << this->log_name() << ", decode key failed, consumed: " << consumed << ", actual: "
                   << ref->key_len();
        return nullptr;
      }
      auto record = std::make_shared<krecord<K, void>>(tmp_key, timestamp);
      return std::make_shared<kevent<K, void>>(record, this->commit_chain_.create(ref->offset()));
    }
  };
}

