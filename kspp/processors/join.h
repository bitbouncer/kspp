#include <functional>
#include "kspp_defs.h"
#pragma once

namespace csi {
//maybe we should materilize the join so we can do get...  
//then we can change type to kstream
template<class K, class streamV, class tableV, class R>
class left_join : public ksource<K, R>
{
  public:
  typedef std::function<void(const K& key, const streamV& left, const tableV& right, R& result)> value_joiner; // TBD replace with std::shared_ptr<const krecord<K, R>> left, std::shared_ptr<const krecord<K, R>> right, std::shared_ptr<krecord<K, R>> result;

  left_join(std::shared_ptr<kstream<K, streamV>> stream, std::shared_ptr<ktable<K, tableV>> table, value_joiner f) :
    _stream(stream),
    _table(table),
    _value_joiner(f) {}

  ~left_join() {
    close();
  }

  std::string name() const { return "left_join-" + _stream->name() + _table->name(); }

  virtual void start() {
    _table->start();
    _stream->start();
  }

  virtual void start(int64_t offset) {
    _table->start();
    _stream->start(offset);
  }

  virtual void close() {
    _table->close();
    _stream->close();
  }
  
  bool consume_right() {
    if (!_table->eof()) {
      _table->consume();
      _table->commit();
      return true;
    }
    return false;
  }

  virtual std::shared_ptr<krecord<K, R>> consume() {
    if (!_table->eof()) {
      // just eat it... no join since we only joins with events????
      _table->consume();
      _table->commit();
      return NULL;
    }

    auto e = _stream->consume();
    if (!e)
      return NULL;

    auto table_row = _table->get(e->key);
    if (table_row) {
      if (e->value) {
        auto p = std::make_shared<csi::krecord<K, R>>(e->key, std::make_shared<R>());
        p->event_time = e->event_time;
        p->offset = e->offset;
        _value_joiner(e->key, *e->value, *table_row->value, *p->value);
        return p;
      } else {
        auto p = std::make_shared<csi::krecord<K, R>>(e->key);
        p->event_time = e->event_time;
        p->offset = e->offset;
        return p;
      }
    } else {
      // join failed
    }
    return NULL;
  }

  virtual void commit() {
    _table->commit();
    _stream->commit();
  }

  virtual bool eof() const {
    return _table->eof() && _stream->eof();
  }

  private:
  std::shared_ptr<kstream<K, streamV>> _stream;
  std::shared_ptr<ktable<K, tableV>>   _table;
  value_joiner                         _value_joiner;
};
}