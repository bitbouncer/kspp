#include <functional>
#include <kspp/kspp.h>
#pragma once

// should this be a partition_sink, topic_source or partition_processor????
// nothing is nessessary but check what's best...
// std::shared_ptr<ktable_partition<K, K>> routing_table could be a external partition-source and an internal ktable_partition that uses an internal kv-store...

namespace kspp {
  template<class K, class V, class PK, class CODEC>
  class repartition_by_table : public partition_processor
  {
  public:
    repartition_by_table(std::shared_ptr<partition_source<K, V>> source, std::shared_ptr<ktable_partition<K, PK>> routing_table, std::shared_ptr<topic_sink<K, V, CODEC>> topic_sink)
      : partition_processor(source.get(), source->partition())
      , _source(source)
      , _routing_table(routing_table)
      , _topic_sink(topic_sink) {
      _source->add_sink([this](auto r) {
        _queue.push_back(r);
      });
      this->add_metric(&_lag);
    }

    ~repartition_by_table() {
      close();
    }

    std::string name() const { 
      return _source->name() + "-repartition_by_value(" + _routing_table->name() + ")"; 
    }

    static std::shared_ptr<partition_processor> create(std::shared_ptr<partition_source<K, V>> source, std::shared_ptr<ktable_partition<K, PK>> routing_table, std::shared_ptr<topic_sink<K, V, CODEC>> topic_sink) {
      return std::make_shared<repartition_by_table<K, V, PK, CODEC>>(source, routing_table, topic_sink);
    }

    virtual std::string processor_name() const { return "repartition_by_table"; }

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
        _lag.add_event_time(e->event_time);
        auto routing_row = _routing_table->get(e->key);
        if (routing_row) {
          if (routing_row->value) {
            uint32_t hash = get_partition_hash<PK, CODEC>(*routing_row->value, _topic_sink->codec());
            _topic_sink->produce(hash, e);
          }
        } else {
          // join failed
        }
      }
      return processed;
    }

    virtual bool eof() const {
      return !_queue.size() && _routing_table->eof() && _source->eof();
    }

  private:
    std::deque<std::shared_ptr<krecord<K, V>>> _queue;
    std::shared_ptr<partition_source<K, V>>    _source;
    std::shared_ptr<ktable_partition<K, PK>>   _routing_table;
    std::shared_ptr<topic_sink<K, V, CODEC>>   _topic_sink;
    metric_lag                                 _lag;
  };
}

