#include <functional>
#include <kspp/kspp.h>
#pragma once

namespace kspp {
template<class K, class V, class FOREIGN_KEY, class CODEC>
class repartition_by_foreign_key : public partition_processor
{
  public:
  repartition_by_foreign_key(topology_base& topology,
                             std::shared_ptr<partition_source<K, V>> source,
                             std::shared_ptr<materialized_source<K, FOREIGN_KEY>> routing_table,
                             std::shared_ptr<topic_sink<K, V>> topic_sink,
                             std::shared_ptr<CODEC> repartition_codec = std::make_shared<CODEC>())
    : partition_processor(source.get(), source->partition())
    , _source(source)
    , _routing_table(routing_table)
    , _topic_sink(topic_sink)
    , _repartition_codec(repartition_codec) {
    _source->add_sink([this](auto r) {
      _queue.push_back(r);
    });
    this->add_metric(&_lag);
  }

  ~repartition_by_foreign_key() {
    close();
  }

  virtual std::string simple_name() const {
    return "repartition_by_foreign_key";
  }

  virtual std::string key_type_name() const {
    return type_name<K>::get();
  }

  virtual std::string value_type_name() const {
    return type_name<V>::get();
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

  virtual bool process_one(int64_t tick) {
    if (!_routing_table->eof()) {
      // just eat it... no join since we only joins with events????
      _routing_table->process_one(tick);
      _routing_table->commit(false);
      return true;
    }

    _source->process_one(tick);
    bool processed = (_queue.size() > 0);
    while (_queue.size()) {
      auto ev = _queue.front();
      _queue.pop_front();
      _lag.add_event_time(tick, ev->event_time());
      auto routing_row = _routing_table->get(ev->record()->key());
      if (routing_row) {
        if (routing_row->value()) {
          uint32_t hash = kspp::get_partition_hash<FOREIGN_KEY, CODEC>(*routing_row->value(), _repartition_codec);
          _topic_sink->produce(hash, ev);
        }
      } else {
        // join failed
      }
    }
    return processed;
  }

  virtual size_t queue_len() {
    return _queue.size();
  }

  virtual bool eof() const {
    return !_queue.size() && _routing_table->eof() && _source->eof();
  }

  virtual void commit(bool force) {
    _routing_table->commit(force);
    _source->commit(force);
  }

  private:
  event_queue<kevent<K, V>>                            _queue;
  std::shared_ptr<partition_source<K, V>>              _source;
  std::shared_ptr<materialized_source<K, FOREIGN_KEY>> _routing_table;
  std::shared_ptr<topic_sink<K, V>>                    _topic_sink;
  std::shared_ptr<CODEC>                               _repartition_codec;
  metric_lag                                           _lag;
};
}

