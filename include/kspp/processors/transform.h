#include <functional>
#include <glog/logging.h>
#include <kspp/kspp.h>
#include <deque>

#pragma once

namespace kspp {
  template<class K, class SV, class RV>
  class transform_value : public event_consumer<K, SV>, public partition_source<K, RV> {
  public:
    typedef std::function<void(std::shared_ptr < kevent < K, SV >> record,
                               transform_value * self)> extractor; // maybe better to pass this and send() directrly

    transform_value(topology &unused, std::shared_ptr <partition_source<K, SV>> source, extractor f)
        : event_consumer<K, SV>()
        , partition_source<K, RV>(source.get(), source->partition())
        , _source(source),
          _extractor(f) {
      _source->add_sink([this](auto r) {
        this->_queue.push_back(r);
      });
      this->add_metric(&_lag);
    }

    ~transform_value() {
      close();
    }

    std::string simple_name() const override {
      return "transform_value";
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
        _lag.add_event_time(tick, trans->event_time());
        this->_queue.pop_front();
        _extractor(trans, this);
      }
      return processed;
    }

    void push_back(std::shared_ptr <kevent<K, RV>> r) {
      this->send_to_sinks(r);
    }

    void commit(bool flush) override {
      _source->commit(flush);
    }

    bool eof() const override {
      return queue_len() == 0 && _source->eof();
    }

    size_t queue_len() const override {
      return event_consumer<K, SV>::queue_len();
    }

  private:
    std::shared_ptr <partition_source<K, SV>> _source;
    extractor _extractor;
    metric_lag _lag;
  };


  template<class K, class V>
  class transform : public event_consumer<K, V>, public partition_source<K, V> {
  public:
    typedef std::function<void(std::shared_ptr < kevent < K, V >> record,
                               transform * self)> extractor; // maybe better to pass this and send() directrly

    transform(topology &unused, std::shared_ptr <partition_source<K, V>> source, extractor f)
        : event_consumer<K, V>()
        , partition_source<K, V>(source.get(), source->partition())
        , _source(source)
        , _extractor(f) {
      _source->add_sink([this](auto r) {
        this->_queue.push_back(r);
      });
      this->add_metric(&_lag);
    }

    ~transform() {
      close();
    }

    std::string simple_name() const override {
      return "transform";
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
        _lag.add_event_time(tick, trans->event_time());
        this->_queue.pop_front();
        _currrent_id = trans->id(); // we capture this to have it in push_back callback
        _extractor(trans, this);
        _currrent_id.reset(); // must be freed otherwise we continue to hold the last ev
      }
      return processed;
    }

    /**
    * use from from extractor callback
    */
    inline void push_back(std::shared_ptr <krecord<K, V>> record) {
      this->send_to_sinks(std::make_shared < kevent < K, V >> (record, _currrent_id));
    }

    void commit(bool flush) override {
      _source->commit(flush);
    }

    bool eof() const override {
      return queue_len() == 0 && _source->eof();
    }

    size_t queue_len() const override {
      return event_consumer<K, V>::queue_len();
    }

  private:
    std::shared_ptr <partition_source<K, V>> _source;
    extractor _extractor;
    std::shared_ptr <commit_chain::autocommit_marker> _currrent_id; // used to briefly hold the commit open during process one
    metric_lag _lag;
  };
}

