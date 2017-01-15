#include <functional>
#include <boost/log/trivial.hpp>
#include <kspp/kspp.h>
#include <deque>
#pragma once

namespace kspp {
  template<class SK, class SV, class RK, class RV>
  class flat_map : public partition_source<RK, RV>
  {
  public:
    typedef std::function<void(std::shared_ptr<krecord<SK, SV>> record, flat_map* self)> extractor; // maybe better to pass this and send() directrly

    flat_map(std::shared_ptr<partition_source<SK, SV>> source, extractor f)
      : partition_source<RK, RV>(source.get(), source->partition())
      , _source(source)
      , _extractor(f)
      , _in_count("in_count")
      , _out_count("out_count") {
      _source->add_sink([this](auto r) {
        _queue.push_back(r);
      });
      add_metrics(&_in_count);
      add_metrics(&_out_count);
      add_metrics(&_lag);
    }

    ~flat_map() {
      close();
    }

    static std::vector<std::shared_ptr<partition_source<RK, RV>>> create(std::vector<std::shared_ptr<partition_source<SK, SV>>>& streams, extractor f) {
      std::vector<std::shared_ptr<partition_source<RK, RV>>> res;
      for (auto i : streams)
        res.push_back(std::make_shared<flat_map<SK, SV, RK, RV>>(i, f));
      return res;
    }

    static std::shared_ptr<partition_source<RK, RV>> create(std::shared_ptr<partition_source<SK, SV>> source, extractor f) {
      return std::make_shared<flat_map<SK, SV, RK, RV>>(source, f);
    }

    std::string name() const {
      return _source->name() + "-flat_map()[" + type_name<RK>::get() + ", " + type_name<RV>::get() + "]"; 
    }

    virtual std::string processor_name() const {
      return "flat_map";
    }

    virtual void start() {
      _source->start();
    }

    virtual void start(int64_t offset) {
      _source->start(offset);
    }

    virtual void close() {
      _source->close();
    }

    virtual bool process_one() {
      _source->process_one();
      bool processed = (_queue.size() > 0);
      while (_queue.size()) {
        auto e = _queue.front();
        _queue.pop_front();
        _lag.add_event_time(e->event_time);
        _extractor(e, this);
        ++_in_count;

      }
      return processed;
    }

    void push_back(std::shared_ptr<krecord<RK, RV>> r) {
      ++_out_count;
      this->send_to_sinks(r);
    }

    virtual void commit() {
      _source->commit();
    }

    virtual bool eof() const {
      return _source->eof() && (_queue.size() == 0);
    }

    virtual bool is_dirty() {
      return (_source->is_dirty() || (_queue.size() == 0));
    }
  
    virtual size_t queue_len() {
      return _queue.size();
    }

  private:
    std::shared_ptr<partition_source<SK, SV>>    _source;
    extractor                                    _extractor;
    std::deque<std::shared_ptr<krecord<SK, SV>>> _queue;
    metrics_counter                              _in_count;
    metrics_counter                              _out_count;
    metrics_lag                                  _lag;
  };

  template<class K, class SV, class RV>
  class transform_value : public partition_source<K, RV>
  {
  public:
    typedef std::function<void(std::shared_ptr<krecord<K, SV>> record, transform_value* self)> extractor; // maybee better to pass this and send() directrly

    transform_value(std::shared_ptr<partition_source<K, SV>> source, extractor f)
      : partition_source<K, RV>(source.get(), source->partition())
      , _source(source)
      , _extractor(f) {
      _source->add_sink([this](auto r) {
        _queue.push_back(r);
      });
      add_metrics(&_lag);
    }

    ~transform_value() {
      close();
    }

    static std::vector<std::shared_ptr<partition_source<K, RV>>> create(std::vector<std::shared_ptr<partition_source<K, SV>>>& streams, extractor f) {
      std::vector<std::shared_ptr<partition_source<K, RV>>> res;
      for (auto i : streams)
        res.push_back(std::make_shared<transform_value<K, SV, RV>>(i, f));
      return res;
    }

    static std::shared_ptr<partition_source<K, RV>> create(std::shared_ptr<partition_source<K, SV>> source, extractor f) {
      return std::make_shared<transform_value<K, SV, RV>>(source, f);
    }

    std::string name() const {
      return _source->name() + "-transform_value";
    }

    virtual std::string processor_name() const {
      return "transform_value";
    }

    virtual void start() {
      _source->start();
    }

    virtual void start(int64_t offset) {
      _source->start(offset);
    }

    virtual void close() {
      _source->close();
    }

    virtual bool process_one() {
      _source->process_one();
      bool processed = (_queue.size() > 0);
      while (_queue.size()) {
        auto e = _queue.front();
        _lag.add_event_time(e->event_time);
        _queue.pop_front();
        _extractor(e, this);
      }
      return processed;
    }

    void push_back(std::shared_ptr<krecord<K, RV>> r) {
      this->send_to_sinks(r);
    }

    virtual void commit() {
      _source->commit();
    }

    virtual bool eof() const {
      return _source->eof() && (_queue.size() == 0);
    }

    virtual bool is_dirty() {
      return (_source->is_dirty() || (_queue.size() == 0));
    }

    virtual size_t queue_len() {
      return _queue.size();
    }

  private:
    std::shared_ptr<partition_source<K, SV>>    _source;
    extractor                                   _extractor;
    std::deque<std::shared_ptr<krecord<K, SV>>> _queue;
    metrics_lag                                 _lag;
  };
}

