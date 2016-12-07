#include <functional>
#include "ktable.h"
#include "kstream.h"
#pragma once

namespace csi {
  template<class K, class tableV, class streamV, class V, class codec>
  class left_join : public ksource<K, V>
  {
  public:
    //typedef std::unique_ptr<krecord<K, V>> record_type;
    //typedef std::function<record_type(const K& key, const tableV& left, const streamV& right, V& value)> assign_row_function;
    typedef std::function<void(const K& key, const tableV& left, const streamV& right, V& value)> assign_row_function;

    left_join(std::shared_ptr<ksource<K, tableV>> table, std::shared_ptr<ksource<K, streamV>> stream, assign_row_function f) :
      _table(table),
      _stream(stream),
      _assign_row(f) {}

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

    virtual std::unique_ptr<krecord<K, V>> consume() {
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
          std::unique_ptr<csi::krecord<K, V>> p(new csi::krecord<K, V>());
          p->value = std::unique_ptr<V>(new V());
          p->key = e->key;
          p->event_time = e->event_time;
          p->offset = e->offset;
          _assign_row(e->key, *table_row->value, *e->value, *p->value);
          return p;
        } else {
          // should null event generate a null event in join??? (likely???)
          // we should forward NULL events here...
          std::unique_ptr<csi::krecord<K, V>> p(new csi::krecord<K, V>());
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
    std::shared_ptr<ksource<K, tableV>>  _table;
    std::shared_ptr<ksource<K, streamV>> _stream;
    assign_row_function                  _assign_row;
  };
}