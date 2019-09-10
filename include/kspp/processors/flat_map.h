#include <functional>
#include <deque>
#include <kspp/kspp.h>

#pragma once

namespace kspp {
  template<class SK, class SV, class RK, class RV>
  class flat_map : public event_consumer<SK, SV>, public partition_source<RK, RV> {
    static constexpr const char* PROCESSOR_NAME = "flat_map";
  public:
    typedef std::function<void(const krecord <SK, SV>& record, flat_map* self)> extractor;

    flat_map(std::shared_ptr<cluster_config> config, std::shared_ptr<partition_source < SK, SV>> source,  extractor f)
    : event_consumer<SK, SV>()
    , partition_source<RK, RV>(source.get(), source->partition())
    , source_(source)
    , extractor_(f) {
      source_->add_sink([this](auto r) { this->_queue.push_back(r); });
      this->add_metrics_label(KSPP_PROCESSOR_TYPE_TAG, PROCESSOR_NAME);
      this->add_metrics_label(KSPP_PARTITION_TAG, std::to_string(source->partition()));
    }

    ~flat_map() {
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
        current_id_ = trans->id(); // we capture this to have it in push_back callback
        if (trans->record())
          extractor_(*trans->record(), this);
        current_id_.reset(); // must be freed otherwise we continue to hold the last ev
      }
      return processed;
    }

    void commit(bool flush) override {
      source_->commit(flush);
    }

    bool eof() const override {
      return ((queue_size() == 0) && source_->eof());
    }

    size_t queue_size() const override {
      return event_consumer<SK, SV>::queue_size();
    }

    int64_t next_event_time() const override {
      return event_consumer<SK, SV>::next_event_time();
    }

    /**
    * use from from extractor callback
    */
    inline void push_back(std::shared_ptr<const krecord<RK, RV>>record) {
      this->send_to_sinks(std::make_shared<kevent<RK, RV>>(record, current_id_));
    }
  /**
     * use from from extractor callback
    */
    inline void push_back(const krecord<RK, RV>& record) {
      auto pr = std::make_shared<krecord<RK, RV>>(record);
      this->send_to_sinks(std::make_shared<kevent<RK, RV>>(pr, current_id_));
    }

    /**
    * use from from extractor callback to force a custom partition hash
    */
    inline void push_back(std::shared_ptr<const krecord<RK, RV>> record, uint32_t partition_hash) {
      this->send_to_sinks(std::make_shared<kevent<RK, RV>>(record, current_id_, partition_hash));
    }

  private:
    std::shared_ptr<partition_source < SK, SV>> source_;
    extractor extractor_;
    std::shared_ptr<event_done_marker> current_id_; // used to briefly hold the commit open during process one
  };
}

template<class SK, class SV, class RK, class RV>
void insert(kspp::flat_map<SK, SV, RK, RV>* flatMap, const kspp::krecord<RK, RV >& r){
  auto kr = std::make_shared<kspp::krecord<RK, RV >>(r);
  flatMap->push_back(kr);
}

template<class SK, class SV, class RK, class RV>
void insert(kspp::flat_map<SK, SV, RK, RV>* flatMap, const RK &k, const RV &v, int64_t ts = kspp::milliseconds_since_epoch()){
  auto kr = std::make_shared<kspp::krecord<RK, RV >>(k, v, ts);
  flatMap->push_back(kr);
}

template<class SK, class SV, class RK, class RV>
void insert(kspp::flat_map<SK, SV, RK, RV>* flatMap, const RK &k, std::shared_ptr<const RV> p, int64_t ts = kspp::milliseconds_since_epoch()){
  auto kr = std::make_shared<kspp::krecord<RK, RV >>(k, p, ts);
  flatMap->push_back(kr);
}

template<class SK, class SV, class RK, class RV>
void insert(kspp::flat_map<SK, SV, RK, RV>* flatMap, const RK &k, std::shared_ptr<RV> p, int64_t ts = kspp::milliseconds_since_epoch()){
  auto kr = std::make_shared<kspp::krecord<RK, RV >>(k, p, ts);
  flatMap->push_back(kr);
}

template<class SK, class SV, class RK, class RV>
void erase(kspp::flat_map<SK, SV, RK, RV>* flatMap, const RK &k, int64_t ts = kspp::milliseconds_since_epoch()){
  auto kr = std::make_shared<kspp::krecord<RK, RV >>(k, nullptr, ts);
  flatMap->push_back(kr);
}

template<class SK, class SV, class RK>
void insert(kspp::flat_map<SK, SV, RK, void>* flatMap, const RK &k, int64_t ts = kspp::milliseconds_since_epoch()){
  auto kr = std::make_shared<kspp::krecord<RK, void>>(k, ts);
  flatMap->push_back(kr);
}

template<class SK, class SV, class RV>
void insert(kspp::flat_map<SK, SV, void, RV>* flatMap, const RV &v, int64_t ts = kspp::milliseconds_since_epoch()){
  auto kr = std::make_shared<kspp::krecord<void, RV >>(v, ts);
  flatMap->push_back(kr);
}

template<class SK, class SV, class RV>
void insert(kspp::flat_map<SK, SV, void, RV>* flatMap, std::shared_ptr<const RV> p, int64_t ts = kspp::milliseconds_since_epoch()){
  auto kr = std::make_shared<kspp::krecord<void, RV >>(p, ts);
  flatMap->push_back(kr);
}

