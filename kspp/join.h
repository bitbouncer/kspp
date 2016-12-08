#include <functional>
#include "ktable.h"
#include "kstream.h"
#pragma once

namespace csi {
  template<class K, class streamV, class tableV, class R>
  class left_join : public ksource<K, R>
  {
  public:
    //typedef std::unique_ptr<krecord<K, V>> record_type;
    //typedef std::function<record_type(const K& key, const tableV& left, const streamV& right, V& value)> assign_row_function;
    typedef std::function<void(const K& key, const streamV& left, const tableV& right, R& result)> value_joiner;

    left_join(std::shared_ptr<ksource<K, streamV>> stream, std::shared_ptr<ksource<K, tableV>> table, value_joiner f) :
      _stream(stream),
      _table(table),
      _value_joiner(f) {}

    ~left_join() {
      close();
    }

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

    virtual std::unique_ptr<krecord<K, R>> consume() {
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
          std::unique_ptr<csi::krecord<K, R>> p(new csi::krecord<K, R>());
          p->value = std::unique_ptr<R>(new R());
          p->key = e->key;
          p->event_time = e->event_time;
          p->offset = e->offset;
          _value_joiner(e->key, *e->value, *table_row->value, *p->value);
          return p;
        } else {
          std::unique_ptr<csi::krecord<K, R>> p(new csi::krecord<K, R>());
          p->key = e->key;
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
    std::shared_ptr<ksource<K, streamV>> _stream;
    std::shared_ptr<ksource<K, tableV>>  _table;
    value_joiner                         _value_joiner;
  };
}