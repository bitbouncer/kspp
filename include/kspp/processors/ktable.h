#include <fstream>
#include <filesystem>
#include <kspp/kspp.h>

#pragma once

namespace kspp {
  template<class K, class V, template<typename, typename, typename> class STATE_STORE, class CODEC=void>
  class ktable : public event_consumer<K, V>, public materialized_source<K, V> {
    static constexpr const char *PROCESSOR_NAME = "ktable";
  public:
    template<typename... Args>
    ktable(std::shared_ptr<cluster_config> config, std::shared_ptr<kspp::partition_source<K, V>> source, Args... args)
        : event_consumer<K, V>(), materialized_source<K, V>(source.get(), source->partition()), source_(source),
          state_store_(this->get_storage_path(config->get_storage_root()), args...),
          state_store_count_("state_store_size", "msg") {
      source_->add_sink([this](auto ev) {
        this->lag_.add_event_time(kspp::milliseconds_since_epoch(), ev->event_time());
        ++(this->processed_count_);
        state_store_.insert(ev->record(), ev->offset());
        this->send_to_sinks(ev);
      });
      // what to do with state_store deleted records (windowed)
      state_store_.set_sink([this](auto ev) {
        this->send_to_sinks(ev);
      });
      this->add_metric(&state_store_count_);
      this->add_metrics_label(KSPP_PROCESSOR_TYPE_TAG, PROCESSOR_NAME);
      this->add_metrics_label(KSPP_PARTITION_TAG, std::to_string(source->partition()));
    }

    ~ktable() override {
      close();
    }

    std::string log_name() const override {
      return PROCESSOR_NAME;
    }

    void start(int64_t offset) override {
      if (offset == kspp::OFFSET_STORED) {
        source_->start(state_store_.offset());
      } else {
        state_store_.start(offset);
        source_->start(offset);
      }
    }

    void commit(bool flush) override {
      state_store_.commit(flush);
    }

    void close() override {
      source_->close();
      state_store_.close();
    }

    bool eof() const override {
      return ((this->queue_.size() == 0) && source_->eof());
    }

    size_t process(int64_t tick) override {
      source_->process(tick);
      size_t processed = 0;

      while (this->queue_.next_event_time() <= tick) {
        auto trans = this->queue_.pop_front_and_get();
        state_store_.insert(trans->record(), trans->offset());
        ++(this->processed_count_);
        ++processed;
        this->send_to_sinks(trans);
      }

      // TODO is this expensive??
      state_store_count_.set(state_store_.aprox_size());
      return processed;
    }

    void garbage_collect(int64_t tick) override {
      state_store_.garbage_collect(tick);
    }

    void garbage_collect_one(int64_t tick) {
      state_store_.garbage_collect_one(tick);
    }

    int64_t offset() const {
      return state_store_.offset();
    }

    size_t queue_size() const override {
      return event_consumer<K, V>::queue_size();
    }

    int64_t next_event_time() const override {
      return event_consumer<K, V>::next_event_time();
    }

    std::shared_ptr<const krecord<K, V>> get(const K &key) const override {
      return state_store_.get(key);
    }

    typename kspp::materialized_source<K, V>::iterator begin(void) const override {
      return state_store_.begin();
    }

    typename kspp::materialized_source<K, V>::iterator end() const override {
      return state_store_.end();
    }

  private:
    std::shared_ptr<kspp::partition_source<K, V>> source_;
    STATE_STORE<K, V, CODEC> state_store_;
    metric_gauge state_store_count_;
  };
}
