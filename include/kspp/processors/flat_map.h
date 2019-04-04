#include <functional>
#include <deque>
#include <kspp/kspp.h>

#pragma once

namespace kspp {
  template<class SK, class SV, class RK, class RV>
  class flat_map : public event_consumer<SK, SV>, public partition_source<RK, RV> {
    static constexpr const char* PROCESSOR_NAME = "flat_map";
  public:
    typedef std::function<void(std::shared_ptr<const krecord <SK, SV>> record, flat_map *self)> extractor;

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
        _currrent_id = trans->id(); // we capture this to have it in push_back callback
        _extractor(trans->record(), this);
        _currrent_id.reset(); // must be freed otherwise we continue to hold the last ev
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
      this->send_to_sinks(std::make_shared<kevent<RK, RV>>(record, _currrent_id));
    }

    /**
    * use from from extractor callback to force a custom partition hash
    */
    inline void push_back(std::shared_ptr<const krecord<RK, RV>> record, uint32_t partition_hash) {
      this->send_to_sinks(std::make_shared<kevent<RK, RV>>(record, _currrent_id, partition_hash));
    }

  private:
    std::shared_ptr<partition_source < SK, SV>> _source;
    extractor _extractor;
    std::shared_ptr<commit_chain::autocommit_marker> _currrent_id; // used to briefly hold the commit open during process one
  };
}

