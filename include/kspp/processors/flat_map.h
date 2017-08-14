#include <functional>
#include <deque>
#include <kspp/kspp.h>
#pragma once

namespace kspp {
  template<class SK, class SV, class RK, class RV>
  class flat_map : public event_consumer<SK, SV>, public partition_source<RK, RV> {
  public:
    typedef std::function<void(std::shared_ptr<const krecord <SK, SV>> record, flat_map *self)> extractor;

    flat_map(topology_base &topology, std::shared_ptr<partition_source < SK, SV>> source, extractor f)
    : event_consumer<SK, SV>()
    , partition_source<RK, RV>(source.get(), source->partition())
    , _source(source)
    , _extractor(f)
    , _in_count("in_count") {
      _source->add_sink([this](auto r) {
        this->_queue.push_back(r);
      });
      this->add_metric(&_in_count);
      this->add_metric(&_lag);
    }

    ~flat_map() {
      close();
    }

    std::string simple_name() const override {
      return "flat_map";
    }

    void start() override {
      _source->start();
    }

    void start(int64_t offset) override {
      _source->start(offset);
    }

    void close() override {
      _source->close();
    }

    bool process_one(int64_t tick) override {
      _source->process_one(tick);
      bool processed = (this->_queue.size() > 0);
      while (this->_queue.size()) {
        auto trans = this->_queue.front();
        this->_queue.pop_front();
        _lag.add_event_time(tick, trans->event_time());
        _currrent_id = trans->id(); // we capture this to have it in push_back callback
        _extractor(trans->record(), this);
        _currrent_id.reset(); // must be freed otherwise we continue to hold the last ev
        ++_in_count;
      }
      return processed;
    }

    void commit(bool flush) override {
      _source->commit(flush);
    }

    bool eof() const override {
      return queue_len() == 0 && _source->eof();
    }

    size_t queue_len() const override {
      return event_consumer < SK, SV > ::queue_len();
    }

    /**
    * use from from extractor callback
    */
    inline void push_back(std::shared_ptr<krecord < RK, RV>> record) {
      this->send_to_sinks(std::make_shared<kevent < RK, RV>>(record, _currrent_id));
    }

  private:
    std::shared_ptr<partition_source < SK, SV>>        _source;
    extractor _extractor;
    std::shared_ptr<commit_chain::autocommit_marker> _currrent_id; // used to briefly hold the commit open during process one
    metric_counter _in_count;
    metric_lag _lag;
  };
}

