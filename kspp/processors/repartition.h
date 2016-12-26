#include <functional>
#include <kspp/kspp_defs.h>
#pragma once

namespace csi {
  template<class K, class V, class CODEC>
  class repartition_by_table : public partition_source<K, V>
  {
  public:
    repartition_by_table(std::shared_ptr<partition_source<K, V>> source, std::shared_ptr<ktable_partition<K, K>> routing_table)
      : partition_source(source->partition())
      , _source(source)
      , _routing_table(routing_table) {}

    ~repartition_by_table() {
      close();
    }

    std::string name() const { return _source->name() + "-repartition_by_value(" + _routing_table->name() + ")"; }

    void add_sink(std::shared_ptr<topic_sink<K, V, CODEC>> sink) {
      _sinks.push_back(sink);
    }

    virtual void start() {
      _routing_table->start();
      _source->start();
    }

    virtual void start(int64_t offset) {
      _routing_table->start();
      _source->start(offset);
    }

    virtual void close() {
      _routing_table->close();
      _source->close();
    }

    bool consume_right() {
      if (!_routing_table->eof()) {
        _routing_table->consume();
        _routing_table->commit();
        return true;
      }
      return false;
    }

    virtual std::shared_ptr<krecord<K, V>> consume() {
      if (!_routing_table->eof()) {
        // just eat it... no join since we only joins with events????
        _routing_table->consume();
        _routing_table->commit();
        return NULL;
      }

      auto e = _source->consume();
      if (!e)
        return NULL;

      auto table_row = _routing_table->get(e->key);
      if (table_row) {
        if (table_row->value) {
          for (auto s : _sinks)
            s->produce(*table_row->value, e);
        }
      } else {
        // join failed
      }
      return NULL;
    }

    virtual void commit() {
      _routing_table->commit();
      _source->commit();
    }

    virtual bool eof() const {
      return _routing_table->eof() && _source->eof();
    }

  private:
    std::shared_ptr<partition_source<K, V>>               _source;
    std::shared_ptr<ktable_partition<K, K>>               _routing_table;
    std::vector<std::shared_ptr<topic_sink<K, V, CODEC>>> _sinks;
  };

  //template<class K, class V>
  //class repartition_table : public ksource<K, V>
  //{
  //  public:
  //  repartition_table(std::shared_ptr<ktable<K, V>> source)
  //    : _source(source) {}

  //  ~repartition_table() {
  //    close();
  //  }

  //  std::string name() const { return _source->name() + "-repartition_table"; }

  //  void add_sink(std::shared_ptr<kpartitionable_sink<K, V>> sink) {
  //    _sinks.push_back(sink);
  //  }

  //  virtual void start() {
  //    _source->start();
  //  }

  //  virtual void start(int64_t offset) {
  //    _source->start(offset);
  //  }

  //  virtual void close() {
  //    _source->close();
  //  }

  //  virtual std::shared_ptr<krecord<K, V>> consume() {
  //    auto e = _source->consume();
  //    if (!e)
  //      return NULL;

  //    auto table_row = _routing_table->get(e->key);
  //    if (table_row) {
  //      if (table_row->value) {
  //        for (auto s : _sinks)
  //          s->produce(*table_row->value, e);
  //      }
  //    } else {
  //      // join failed
  //    }
  //    return NULL;
  //  }

  //  virtual void commit() {
  //    _routing_table->commit();
  //    _source->commit();
  //  }

  //  virtual bool eof() const {
  //    return _routing_table->eof() && _source->eof();
  //  }

  //  private:
  //  std::shared_ptr<ksource<K, V>>                          _source;
  //  std::shared_ptr<ktable<K, K>>                           _routing_table;
  //  std::vector<std::shared_ptr<kpartitionable_sink<K, V>>> _sinks;
  //};
  //
}

