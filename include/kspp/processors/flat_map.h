#include <functional>
#include <deque>
#include <kspp/kspp.h>

#pragma once

namespace kspp {
  template<class SK, class SV, class RK, class RV>
  class flat_map : public event_consumer<SK, SV>, public partition_source<RK, RV> {
    static constexpr const char* PROCESSOR_NAME = "flat_map";
  public:
    //typedef std::function<void(std::shared_ptr<const krecord <SK, SV>> record, flat_map *self)> extractor;
    typedef std::function<void(const krecord <SK, SV>& record, flat_map *self)> extractor;

    flat_map(std::shared_ptr<cluster_config> config, std::shared_ptr<partition_source < SK, SV>> source,  extractor f)
    : event_consumer<SK, SV>()
    , partition_source<RK, RV>(source.get(), source->partition())
    , _source(source)
    , _extractor(f) {
      _source->add_sink([this](auto r) { this->_queue.push_back(r); });
      this->add_metrics_label(KSPP_PROCESSOR_TYPE_TAG, "flat_map");
      this->add_metrics_label(KSPP_PARTITION_TAG, std::to_string(source->partition()));
    }

    ~flat_map() {
      close();
    }

    std::string log_name() const override {
      return PROCESSOR_NAME;
    }

    void start(int64_t offset) override {
      _source->start(offset);
    }

    void close() override {
      _source->close();
    }

    size_t process(int64_t tick) override {
      _source->process(tick);
      size_t processed=0;
      while (this->_queue.next_event_time()<=tick){
        auto trans = this->_queue.pop_and_get();
        ++processed;
        this->_lag.add_event_time(tick, trans->event_time());
        ++(this->_processed_count);
        _current_id = trans->id(); // we capture this to have it in push_back callback
        if (trans->record())
          _extractor(*trans->record(), this);
        _current_id.reset(); // must be freed otherwise we continue to hold the last ev
      }
      return processed;
    }

    void commit(bool flush) override {
      _source->commit(flush);
    }

    bool eof() const override {
      return queue_size() == 0 && _source->eof();
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
      this->send_to_sinks(std::make_shared<kevent<RK, RV>>(record, _current_id));
    }

    /*
     * inline void push_back(const RK& key, std::shared_ptr<RV> row, int64_t ts = milliseconds_since_epoch()) {
      auto pr = std::make_shared<krecord<RK, RV>>(key, row, ts);
      this->send_to_sinks(std::make_shared<kevent<RK, RV>>(pr, _current_id));
    }

    inline void push_back(const RK& key, const RV& row, int64_t ts = milliseconds_since_epoch()) {
      auto pr = std::make_shared<krecord<RK, RV>>(key, row, ts);
      this->send_to_sinks(std::make_shared<kevent<RK, RV>>(pr, _current_id));
    }
     */

    /**
     * use from from extractor callback
    */
    inline void push_back(const krecord<RK, RV>& record) {
      auto pr = std::make_shared<krecord<RK, RV>>(record);
      this->send_to_sinks(std::make_shared<kevent<RK, RV>>(pr, _current_id));
    }

    /**
    * use from from extractor callback to force a custom partition hash
    */
    inline void push_back(std::shared_ptr<const krecord<RK, RV>> record, uint32_t partition_hash) {
      this->send_to_sinks(std::make_shared<kevent<RK, RV>>(record, _current_id, partition_hash));
    }

  private:
    std::shared_ptr<partition_source < SK, SV>> _source;
    extractor _extractor;
    std::shared_ptr<commit_chain::autocommit_marker> _current_id; // used to briefly hold the commit open during process one
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

