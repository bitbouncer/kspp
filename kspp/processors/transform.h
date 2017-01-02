#include <functional>
#include <boost/log/trivial.hpp>
#include <kspp/kspp_defs.h>
#include <deque>
#pragma once

namespace csi {
template<class SK, class SV, class RK, class RV>
class transform_stream : public partition_source<RK, RV>
{
  public:
  typedef std::function<void(std::shared_ptr<krecord<SK, SV>> record, transform_stream* self)> extractor; // maybee better to pass this and send() directrly

  transform_stream(std::shared_ptr<partition_source<SK, SV>> source, extractor f)
    : partition_source(source->partition())
    , _source(source)
    , _extractor(f) {
    _source->add_sink([this](auto r) {
      _queue.push_back(r);
    });
  }

  ~transform_stream() {
    close();
  }

  /*
  static std::vector<std::shared_ptr<transform_stream<SK, SV, RK, RV>>> create(std::vector<std::shared_ptr<ksource<SK, SV>>>& streams, extractor f) {
    std::vector< std::shared_ptr<transform_stream<RK, RV>> res;
    for (auto i : streams)
      res.push_back(std::make_shared<transform_stream<RK, RV>>(i, f));
    return res;
  }
  */

  static std::vector<std::shared_ptr<partition_source<RK, RV>>> create(std::vector<std::shared_ptr<partition_source<SK, SV>>>& streams, extractor f) {
    std::vector<std::shared_ptr<partition_source<RK, RV>>> res;
    for (auto i : streams)
      res.push_back(std::make_shared<transform_stream<SK, SV, RK, RV>>(i, f));
    return res;
  }

  static std::shared_ptr<partition_source<RK, RV>> create(std::shared_ptr<partition_source<SK, SV>> source, extractor f) {
    return std::make_shared<transform_stream<SK, SV, RK, RV>>(source, f);
  }

  std::string name() const {
    return _source->name() + "-transform_stream";
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

  virtual size_t queue_len() {
    return _queue.size();
  }
  virtual std::string topic() const {
    return "internal-deque";
  }

  virtual void poll(int timeout) {
    // do nothing
  }

  private:
  std::shared_ptr<partition_source<SK, SV>>    _source;
  extractor                                    _extractor;
  std::deque<std::shared_ptr<krecord<SK, SV>>> _queue;
};
}

