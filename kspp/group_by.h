#include <functional>
#include <boost/log/trivial.hpp>
#include "kspp_defs.h"
#include <deque>
#include "counter_store.h"
#pragma once

namespace csi {
//template<class K, class V, class RK>
//class group_by_value : public ksource<RK, size_t>, private ksink<RK, size_t>
//{
//  public:
//  typedef std::function<void(std::shared_ptr<krecord<RK, size_t>>, ksink<RK, size_t>* self)> value_extractor;
//  //typedef std::function<void(const K& key, const V& value, ksink<RK, size_t> self)> value_extractor;
//
//  group_by_value(std::shared_ptr<ksource<K, V>> source, value_extractor f) :
//    _stream(source),
//    _value_extractor(f) {}
//
//  ~group_by_value() {
//    close();
//  }
//
//  virtual void start() {
//    _stream->start();
//  }
//
//  virtual void start(int64_t offset) {
//    _stream->start(offset);
//  }
//
//  virtual void close() {
//    _stream->close();
//  }
//
//  virtual std::shared_ptr<krecord<RK, size_t>> consume() {
//    auto e = _stream->consume();
//    if (!e)
//      return NULL;
//
//    // we cannot handle null values???
//    if (!e->value)
//      return NULL;
//
//    //_value_extractor(e->key, *e->value, )
//    _value_extractor(*e->value, this);
//
//    /*
//    auto table_row = _table->get(e->key);
//    if (table_row) {
//      if (e->value) {
//        auto p = std::make_shared<csi::krecord<K, R>>(e->key, std::make_shared<R>());
//        p->event_time = e->event_time;
//        p->offset = e->offset;
//        _value_joiner(e->key, *e->value, *table_row->value, *p->value);
//        return p;
//      } else {
//        auto p = std::make_shared<csi::krecord<K, R>>(e->key);
//        p->event_time = e->event_time;
//        p->offset = e->offset;
//        return p;
//      }
//    } else {
//      // join failed
//    }
//    */
//    return NULL;
//  }
//
//  virtual void commit() {
//    _stream->commit();
//  }
//
//  virtual bool eof() const {
//    return _stream->eof();
//  }
//
//  // PRIVATE INTERFACE SINK
//  private:
//  virtual int produce(std::shared_ptr<krecord<RK, size_t>> r) {
//    BOOST_LOG_TRIVIAL(info) << "key:" << r->key << ", count:" << *r->value;
//    return 0;
//  }
//
//  virtual size_t queue_len() {
//    // not implemented
//    return 0;
//  }
//  virtual std::string topic() const { 
//    return "internal - ram"; 
//  }
//  
//  virtual void poll(int timeout) {
//    // do nothing
//  }
//
//  private:
//  std::shared_ptr<ksource<K, V>> _stream;
//  value_extractor                _value_extractor;
//};

template<class SK, class SV, class RK, class RV>
class transform_stream : public ksource<RK, RV>, private ksink<RK, RV>
{
  public:
  typedef std::function<void(std::shared_ptr<krecord<SK, SV>> record, ksink<RK, RV>* self)> extractor;
  //typedef std::function<void(const K& key, const V& value, ksink<RK, size_t> self)> value_extractor;

  transform_stream(std::shared_ptr<ksource<SK, SV>> source, extractor f) :
    _stream(source),
    _extractor(f) {}

  ~transform_stream() {
    close();
  }

  std::string name() const { return "transform_stream-" + _stream->name(); }

  virtual void start() {
    _stream->start();
  }

  virtual void start(int64_t offset) {
    _stream->start(offset);
  }

  virtual void close() {
    _stream->close();
  }

  virtual std::shared_ptr<krecord<RK, RV>> consume() {
    if (_queue.size()) {
      auto p = *_queue.begin();
      _queue.pop_front();
      return p;
    }
    
    auto e = _stream->consume();
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
    _stream->commit();
  }

  virtual bool eof() const {
    return _stream->eof() && (_queue.size() == 0);
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
  std::shared_ptr<ksource<SK, SV>>             _stream;
  extractor                                    _extractor;
  std::deque<std::shared_ptr<krecord<RK, RV>>> _queue;
};

// COUNT KEYS
template<class K, class codec>
class count_keys : public ksource<K, size_t>
{
  public:
  count_keys(std::shared_ptr<ksource<K, void>> source, std::string storage_path, std::shared_ptr<codec> codec) :
    _stream(source),
    _counter_store(name(), storage_path + "//" + name(), codec)
    {}

  ~count_keys() {
    close();
  }

  std::string name() const {
    return "count_keys-" + _stream->name();
  }

  virtual void start() {
    _stream->start();
  }

  virtual void start(int64_t offset) {
    _stream->start(offset);
  }

  virtual void close() {
    _stream->close();
  }

  virtual std::shared_ptr<krecord<K, size_t>> consume() {
    if (_queue.size()) {
      auto p = *_queue.begin();
      _queue.pop_front();
      return p;
    }

    auto e = _stream->consume();
    if (!e)
      return NULL;

   _counter_store.add(e->key, 1);
   size_t count = _counter_store.get(e->key);
   return std::make_shared<krecord<K, size_t>>(e->key, count);
  }

  virtual void commit() {
    _stream->commit();
  }

  virtual bool eof() const {
    return _queue.size() ==0 && _stream->eof();
  }

  private:
  std::shared_ptr<ksource<K, void>>               _stream;
  kkeycounter_store<K, codec>                     _counter_store;
  std::deque<std::shared_ptr<krecord<K, size_t>>> _queue;
};

}

