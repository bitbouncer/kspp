#include <functional>
#include <kspp/kspp.h>

#pragma once

//TBD is this not a sink???

namespace kspp {
  template<class K, class V, class FOREIGN_KEY, class CODEC>
  class repartition_by_foreign_key : public event_consumer<K, V>, public partition_processor {
  public:
    repartition_by_foreign_key(topology_base &topology,
                               std::shared_ptr<partition_source < K, V>> source,
    std::shared_ptr<materialized_source < K, FOREIGN_KEY>> routing_table,
    std::shared_ptr<topic_sink < K, V>> topic_sink,
    std::shared_ptr<CODEC> repartition_codec = std::make_shared<CODEC>())
    : event_consumer<K, V>()
    , partition_processor(source.get(), source->partition())
    , _source (source)
    , _routing_table(routing_table)
    , _topic_sink(topic_sink)
    , _repartition_codec(repartition_codec) {
      _source->add_sink([this](auto r) {
        this->_queue.push_back(r);
      });
      this->add_metric(&_lag);
    }

    ~repartition_by_foreign_key() {
      close();
    }

    std::string simple_name() const override {
      return "repartition_by_foreign_key";
    }

    std::string key_type_name() const override {
      return type_name<K>::get();
    }

    std::string value_type_name() const override {
      return type_name<V>::get();
    }

    void start() override {
      _routing_table->start();
      _source->start();
    }

    void start(int64_t offset) override {
      _routing_table->start();
      _source->start(offset);
    }

    void close() override {
      _routing_table->close();
      _source->close();
    }

    bool process_one(int64_t tick) override {
      if (!_routing_table->eof()) {
        // just eat it... no join since we only joins with events????
        _routing_table->process_one(tick);
        _routing_table->commit(false);
        return true;
      }

      _source->process_one(tick);
      bool processed = (this->_queue.size() > 0);
      while (this->_queue.size()) {
        auto ev = this->_queue.front();
        this->_queue.pop_front();
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

    size_t queue_len() const override {
      return event_consumer < K, V > ::queue_len();
    }

    bool eof() const override {
      return queue_len() == 0 && _routing_table->eof() && _source->eof();
    }

    void commit(bool force) override {
      _routing_table->commit(force);
      _source->commit(force);
    }

  private:
    std::shared_ptr<partition_source < K, V>> _source;
    std::shared_ptr<materialized_source < K, FOREIGN_KEY>> _routing_table;
    std::shared_ptr<topic_sink < K, V>> _topic_sink;
    std::shared_ptr<CODEC> _repartition_codec;
    metric_lag _lag;
  };
}

