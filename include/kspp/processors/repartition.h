#include <functional>
#include <kspp/kspp.h>

#pragma once

//TBD is this not a sink???

namespace kspp {
  template<class K, class V, class FOREIGN_KEY, class CODEC>
  class repartition_by_foreign_key : public event_consumer<K, V>, public partition_processor {
    static constexpr const char* PROCESSOR_NAME = "repartition_by_foreign_key";
  public:
    repartition_by_foreign_key(topology &t,
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
      this->add_metrics_tag(KSPP_PROCESSOR_TYPE_TAG, "repartition_by_foreign_key");
      this->add_metrics_tag(KSPP_PARTITION_TAG, std::to_string(source->partition()));
    }

    ~repartition_by_foreign_key() {
      close();
    }

    std::string log_name() const override {
      return PROCESSOR_NAME;
    }

    std::string key_type_name() const override {
      return type_name<K>::get();
    }

    std::string value_type_name() const override {
      return type_name<V>::get();
    }

    void start(int64_t offset) override {
      _routing_table->start(kspp::OFFSET_STORED);
      _source->start(offset);
    }

    void close() override {
      _routing_table->close();
      _source->close();
    }

    size_t process(int64_t tick) override {
      if (_routing_table->process(tick)>0)
        _routing_table->commit(false);

      _source->process(tick);
      size_t processed = 0;
      while (this->_queue.next_event_time()<=tick) {
        auto trans = this->_queue.pop_and_get();
        this->_lag.add_event_time(tick, trans->event_time());
        ++(this->_processed_count);
        auto routing_row = _routing_table->get(trans->record()->key());
        if (routing_row) {
          if (routing_row->value()) {
            uint32_t hash = kspp::get_partition_hash<FOREIGN_KEY, CODEC>(*routing_row->value(), _repartition_codec);
            _topic_sink->push_back(hash, trans);
            ++processed;
          }
        } else {
          // join failed
        }
      }
      return processed;
    }

    size_t queue_size() const override {
      return event_consumer < K, V > ::queue_size();
    }

    int64_t next_event_time() const override {
      return event_consumer<K, V>::next_event_time();
    }

    bool eof() const override {
      return queue_size() == 0 && _routing_table->eof() && _source->eof();
    }

    void commit(bool force) override {
      _routing_table->commit(force);
      _source->commit(force);
    }

  private:
    std::shared_ptr<partition_source<K, V>> _source;
    std::shared_ptr<materialized_source<K, FOREIGN_KEY>> _routing_table;
    std::shared_ptr<topic_sink<K, V>> _topic_sink;
    std::shared_ptr<CODEC> _repartition_codec;
  };
}

