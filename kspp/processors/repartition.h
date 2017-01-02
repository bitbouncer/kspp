#include <functional>
#include <kspp/kspp_defs.h>
#pragma once

// should this be a partition_sink, topic_source or partition_processor????
// nothing is nessessary but check what's best...
// std::shared_ptr<ktable_partition<K, K>> routing_table could be a external partition-source and an internal ktable_partition that uses an internal kv-store...

namespace csi {
  template<class K, class V, class CODEC>
  class repartition_by_table : partition_processor
  {
  public:
    repartition_by_table(std::shared_ptr<partition_source<K, V>> source, std::shared_ptr<ktable_partition<K, K>> routing_table, std::shared_ptr<topic_sink<K, V, CODEC>> topic_sink)
      : partition_processor(source->partition())
      , _source(source)
      , _routing_table(routing_table)
      , _topic_sink(topic_sink) {
      _source->add_sink([this](auto r) {
        _queue.push_back(r);
      });
    }

    ~repartition_by_table() {
      close();
    }

    std::string name() const { return _source->name() + "-repartition_by_value(" + _routing_table->name() + ")"; }

    /*
    void add_sink(std::shared_ptr<topic_sink<K, V, CODEC>> sink) {
      _sinks.push_back(sink);
    }
    */

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
        _routing_table->process_one();
        _routing_table->commit();
        return true;
      }
      return false;
    }

    virtual bool process_one() {
      if (!_routing_table->eof()) {
        // just eat it... no join since we only joins with events????
        _routing_table->process_one();
        _routing_table->commit();
        return true;
      }

      _source->process_one();
      bool processed = (_queue.size() > 0);
      while (_queue.size()) {
        auto e = _queue.front();
        _queue.pop_front();
        auto table_row = _routing_table->get(e->key);
        if (table_row) {
          if (table_row->value) {
            _topic_sink->produce(*table_row->value, e);
          }
        } else {
          // join failed
        }
      }
      return processed;
    }

    virtual bool eof() const {
      return _routing_table->eof() && _source->eof();
    }

  private:
    std::deque<std::shared_ptr<krecord<K, V>>> _queue;
    std::shared_ptr<partition_source<K, V>>    _source;
    std::shared_ptr<ktable_partition<K, K>>    _routing_table;
    std::shared_ptr<topic_sink<K, V, CODEC>>   _topic_sink;
  };
}

