#include <functional>
#include <kspp/kspp.h>

#pragma once

//TBD is this not a sink???

namespace kspp {
  template<class K, class V, class FOREIGN_KEY, class CODEC>
  class repartition_by_foreign_key : public event_consumer<K, V>, public partition_processor {
    static constexpr const char* PROCESSOR_NAME = "repartition_by_foreign_key";
  public:
    repartition_by_foreign_key(std::shared_ptr<cluster_config> config,
        std::shared_ptr<partition_source < K, V>> source,
    std::shared_ptr<materialized_source < K, FOREIGN_KEY>> routing_table,
    std::shared_ptr<topic_sink < K, V>> topic_sink,
    std::shared_ptr<CODEC> repartition_codec = std::make_shared<CODEC>())
    : event_consumer<K, V>()
    , partition_processor(source.get(), source->partition())
    , source_ (source)
    , routing_table_(routing_table)
    , topic_sink_(topic_sink)
    , repartition_codec_(repartition_codec) {
      source_->add_sink([this](auto r) {
        this->_queue.push_back(r);
      });
      this->add_metric(&_lag);
      this->add_metrics_label(KSPP_PROCESSOR_TYPE_TAG, "repartition_by_foreign_key");
      this->add_metrics_label(KSPP_PARTITION_TAG, std::to_string(source->partition()));
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
      routing_table_->start(kspp::OFFSET_STORED);
      source_->start(offset);
    }

    void close() override {
      routing_table_->close();
      source_->close();
    }

    size_t process(int64_t tick) override {
      if (routing_table_->process(tick)>0)
        routing_table_->commit(false);

      source_->process(tick);
      size_t processed = 0;
      while (this->_queue.next_event_time()<=tick) {
        auto trans = this->_queue.pop_front_and_get();
        this->_lag.add_event_time(tick, trans->event_time());
        ++(this->_processed_count);
        auto routing_row = routing_table_->get(trans->record()->key());
        if (routing_row) {
          if (routing_row->value()) {
            uint32_t hash = kspp::get_partition_hash<FOREIGN_KEY, CODEC>(*routing_row->value(), repartition_codec_);
            topic_sink_->push_back(hash, trans);
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
      return ((queue_size() == 0) && routing_table_->eof() && source_->eof());
    }

    void commit(bool force) override {
      routing_table_->commit(force);
      source_->commit(force);
    }

  private:
    std::shared_ptr<partition_source<K, V>> source_;
    std::shared_ptr<materialized_source<K, FOREIGN_KEY>> routing_table_;
    std::shared_ptr<topic_sink<K, V>> topic_sink_;
    std::shared_ptr<CODEC> repartition_codec_;
  };
}

