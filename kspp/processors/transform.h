#include <functional>
#include <boost/log/trivial.hpp>
#include <kspp/kspp_defs.h>
#include <deque>
#pragma once

namespace csi {
template<class SK, class SV, class RK, class RV>
class transform_stream : public ksource<RK, RV>, private ksink<RK, RV>
{
  public:
  typedef std::function<void(std::shared_ptr<krecord<SK, SV>> record, ksink<RK, RV>* self)> extractor;
  //typedef std::function<void(const K& key, const V& value, ksink<RK, size_t> self)> value_extractor;

  transform_stream(std::shared_ptr<ksource<SK, SV>> source, extractor f) :
    _source(source),
    _extractor(f) {}

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

  static std::vector<std::shared_ptr<ksource<RK, RV>>> create(std::vector<std::shared_ptr<ksource<SK, SV>>>& streams, extractor f) {
    std::vector<std::shared_ptr<ksource<RK, RV>>> res;
    for (auto i : streams)
      res.push_back(std::make_shared<transform_stream<SK, SV, RK, RV>>(i, f));
    return res;
  }

  static std::shared_ptr<ksource<RK, RV>> create(std::shared_ptr<ksource<SK, SV>> source, extractor f) {
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

  virtual std::shared_ptr<krecord<RK, RV>> consume() {
    if (_queue.size()) {
      auto p = *_queue.begin();
      _queue.pop_front();
      return p;
    }
    
    auto e = _source->consume();
    if (e)
      _extractor(e, this);

    if (_queue.size()) {
      auto p = *_queue.begin();
      _queue.pop_front();
      return p;
    }

    return NULL;
  }

  virtual void commit() {
    _source->commit();
  }

  virtual bool eof() const {
    return _source->eof() && (_queue.size() == 0);
  }

  // PRIVATE INTERFACE SINK
  private:
  virtual int produce(std::shared_ptr<krecord<RK, RV>> r) {
    _queue.push_back(r);
    return 0;
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
  std::shared_ptr<ksource<SK, SV>>             _source;
  extractor                                    _extractor;
  std::deque<std::shared_ptr<krecord<RK, RV>>> _queue;
};
}

