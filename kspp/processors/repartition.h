#include <functional>
#include <kspp/kspp_defs.h>
#pragma once

namespace csi {
  template<class K, class V, class tableV>
  class repartition_stream : public ksource<K, V>
  {
  public:
    typedef std::function<int32_t(const tableV& right)> partitioner;

    repartition_stream(std::shared_ptr<ksource<K, V>> source, std::shared_ptr<ktable<K, tableV>> table, partitioner f)
      : _source(source)
      , _table(table)
      , _sink(sink)
      , _partitioner(f) {}

    ~repartition_stream() {
      close();
    }

    std::string name() const { return _source->name() + "-repartition_stream-on(" + _table->name() +")"; }

    void add_sink(std::shared_ptr<ksink<K, V>> sink) {
      _sinks.push_back(sink);
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

    bool consume_right() {
      if (!_table->eof()) {
        _table->consume();
        _table->commit();
        return true;
      }
      return false;
    }

    virtual std::shared_ptr<krecord<K, V>> consume() {
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
          int32_t p = _partitioner(*table_row->value);
          for (auto s : _sinks)
            s->produce(p, e);
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
    std::shared_ptr<ksource<K, V>>            _source;
    // kv store K, size_t (partition)
    std::shared_ptr<ktable<K, tableV>>        _table;
    partitioner                               _partitioner;
    std::vector<std::shared_ptr<ksink<K, V>>> _sinks;
  };
}