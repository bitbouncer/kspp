#include <functional>
#include <boost/log/trivial.hpp>
#include "kspp_defs.h"
#include <deque>
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
}

