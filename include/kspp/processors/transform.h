#include <functional>
#include <glog/logging.h>
#include <kspp/kspp.h>
#include <deque>

#pragma once

namespace kspp {
  template<class K, class SV, class RV>
  class transform_value : public event_consumer<K, SV>, public partition_source<K, RV> {
    static constexpr const char* PROCESSOR_NAME = "transform_value";
  public:
    //typedef std::function<void(std::shared_ptr<const krecord <K, SV>> record, transform_value *self)> extractor;
    typedef std::function<void(const krecord <K, SV>& record, transform_value *self)> extractor;

    transform_value(std::shared_ptr<cluster_config> config, std::shared_ptr <partition_source<K, SV>> source, extractor f)
        : event_consumer<K, SV>()
        , partition_source<K, RV>(source.get(), source->partition())
        , source_(source),
    extractor_(f) {
      source_->add_sink([this](auto r) {
        this->_queue.push_back(r);
      });
      this->add_metrics_label(KSPP_PROCESSOR_TYPE_TAG, PROCESSOR_NAME);
      this->add_metrics_label(KSPP_PARTITION_TAG, std::to_string(source->partition()));
    }

    ~transform_value() {
      close();
    }

    std::string simple_name() const override {
      return PROCESSOR_NAME;
    }

    void start(int64_t offset) override {
      source_->start(offset);
    }

    void close() override {
      source_->close();
    }

    size_t process(int64_t tick) override {
      source_->process(tick);
      size_t processed=0;
      while (this->_queue.next_event_time()<=tick){
        auto trans = this->_queue.pop_front_and_get();
        ++processed;
        this->_lag.add_event_time(tick, trans->event_time());
        ++(this->_processed_count);
        currrent_id_ = trans->id(); // we capture this to have it in push_back callback
        if (trans->record())
          extractor_(*trans->record(), this);
        currrent_id_.reset(); // must be freed otherwise we continue to hold the last ev
      }
      return processed;
    }

    /**
    * use from from extractor callback
    */
    inline void push_back(std::shared_ptr<krecord<K, RV>>record) {
      this->send_to_sinks(std::make_shared<kevent<K, RV>>(record, currrent_id_));
    }


    /*void push_back(std::shared_ptr <kevent<K, RV>> r) {
      this->send_to_sinks(r);
    }*/


    void commit(bool flush) override {
      source_->commit(flush);
    }

    bool eof() const override {
      return ((queue_size() == 0) && source_->eof());
    }

    size_t queue_size() const override {
      return event_consumer<K, SV>::queue_size();
    }

  private:
    std::shared_ptr <partition_source<K, SV>> source_;
    extractor extractor_;
    std::shared_ptr<commit_chain::autocommit_marker> currrent_id_; // used to briefly hold the commit open during process one
  };


  template<class K, class V>
  class transform : public event_consumer<K, V>, public partition_source<K, V> {
    static constexpr const char* PROCESSOR_NAME = "transform";
  public:
    //typedef std::function<void(std::shared_ptr<const krecord <K, V>> record, transform *self)> extractor;
    typedef std::function<void(const krecord <K, V>& record, transform *self)> extractor;

    transform(topology &unused, std::shared_ptr <partition_source<K, V>> source, extractor f)
        : event_consumer<K, V>()
        , partition_source<K, V>(source.get(), source->partition())
        , source_(source)
        , extractor_(f) {
      source_->add_sink([this](auto r) {
        this->_queue.push_back(r);
      });
      this->add_metrics_label(KSPP_PROCESSOR_TYPE_TAG, PROCESSOR_NAME);
      this->add_metrics_label(KSPP_PARTITION_TAG, std::to_string(source->partition()));
    }

    ~transform() {
      close();
    }

    std::string log_name() const override {
      return PROCESSOR_NAME;
    }

    void start(int64_t offset) override {
      source_->start(offset);
    }

    void close() override {
      source_->close();
    }

    size_t process(int64_t tick) override {
      source_->process(tick);
      size_t processed=0;
      while (this->_queue.next_event_time()<=tick){
        auto trans = this->_queue.pop_front_and_get();
        ++processed;
        this->_lag.add_event_time(tick, trans->event_time());
        ++(this->_processed_count);
        currrent_id_ = trans->id(); // we capture this to have it in push_back callback
        if (trans->record())
          extractor_(*trans->record(), this);
        currrent_id_.reset(); // must be freed otherwise we continue to hold the last ev
      }
      return processed;
    }

    /**
    * use from from extractor callback
    */
    inline void push_back(std::shared_ptr<krecord<K, V>>record) {
      this->send_to_sinks(std::make_shared<kevent<K, V>>(record, currrent_id_));
    }

    void commit(bool flush) override {
      source_->commit(flush);
    }

    bool eof() const override {
      return queue_size() == 0 && source_->eof();
    }

    size_t queue_size() const override {
      return event_consumer<K, V>::queue_size();
    }

    int64_t next_event_time() const override {
      return event_consumer<K, V>::next_event_time();
    }

  private:
    std::shared_ptr <partition_source<K, V>> source_;
    extractor extractor_;
    std::shared_ptr <commit_chain::autocommit_marker> currrent_id_; // used to briefly hold the commit open during process one
  };
}

