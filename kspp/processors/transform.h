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
      , _extractor(f) {
      _source->add_sink([this](auto r) {
        _queue.push_back(r);
      });
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
      return _source->name() + "-flat_map";
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
        _extractor(e, this);
      }
      return processed;
    }

    void push_back(std::shared_ptr<krecord<RK, RV>> r) {
      send_to_sinks(r);
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

    virtual std::string topic() const {
      return "internal-deque";
    }

  private:
    std::shared_ptr<partition_source<SK, SV>>    _source;
    extractor                                    _extractor;
    std::deque<std::shared_ptr<krecord<SK, SV>>> _queue;
  };

  template<class K, class SV, class RV>
  class transform_value : public partition_source<K, RV>
  {
  public:
    typedef std::function<void(std::shared_ptr<krecord<K, SV>> record, transform_value* self)> extractor; // maybee better to pass this and send() directrly

    transform_value(std::shared_ptr<partition_source<K, SV>> source, extractor f)
      : partition_source<K, RV>(source->partition())
      , _source(source)
      , _extractor(f) {
      _source->add_sink([this](auto r) {
        _queue.push_back(r);
      });
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
        _extractor(e, this);
      }
      return processed;
    }

    void push_back(std::shared_ptr<krecord<K, RV>> r) {
      send_to_sinks(r);
    }

    virtual void commit() {
      _source->commit();
    }

    virtual bool eof() const {
      return _source->eof() && (_queue.size() == 0);
    }

    virtual size_t queue_len() {
      return _queue.size();
    }

    virtual std::string topic() const {
      return "internal-deque";
    }

  private:
    std::shared_ptr<partition_source<K, SV>>    _source;
    extractor                                   _extractor;
    std::deque<std::shared_ptr<krecord<K, SV>>> _queue;
  };
}

