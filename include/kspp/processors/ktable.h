#include <fstream>
#include <boost/filesystem.hpp>
#include <kspp/kspp.h>

#pragma once

namespace kspp {
  template<class K, class V, template<typename, typename, typename> class STATE_STORE, class CODEC=void>
  class ktable : public event_consumer<K, V>, public materialized_source<K, V> {
    static constexpr const char* PROCESSOR_NAME = "ktable";
  public:
    template<typename... Args>
    ktable(std::shared_ptr<cluster_config> config, std::shared_ptr<kspp::partition_source<K, V>> source, Args... args)
            : event_consumer<K, V>(), materialized_source<K, V>(source.get(), source->partition()), _source(source),
              _state_store(this->get_storage_path(config->get_storage_root()), args...),
              _state_store_count("state_store_size", "msg") {
      _source->add_sink([this](auto ev) {
        this->_lag.add_event_time(kspp::milliseconds_since_epoch(), ev->event_time());
        ++(this->_processed_count);
        _state_store.insert(ev->record(), ev->offset());
        this->send_to_sinks(ev);
      });
      // what to do with state_store deleted records (windowed)
      _state_store.set_sink([this](auto ev) {
        this->send_to_sinks(ev);
      });
      this->add_metric(&_state_store_count);
      this->add_metrics_tag(KSPP_PROCESSOR_TYPE_TAG, PROCESSOR_NAME);
      this->add_metrics_tag(KSPP_PARTITION_TAG, std::to_string(source->partition()));
    }

    ~ktable() override {
      close();
    }

    std::string log_name() const override {
      return PROCESSOR_NAME;
    }

    void start(int64_t offset) override {
      if (offset==kspp::OFFSET_STORED) {
        _source->start(_state_store.offset());
      } else {
        _state_store.start(offset);
        _source->start(offset);
      }
    }

    void commit(bool flush) override {
      _state_store.commit(flush);
    }

    void close() override {
      _source->close();
      _state_store.close();
    }

    bool eof() const override {
      return this->_queue.size()==0 && _source->eof();
    }

    size_t process(int64_t tick) override {
      _source->process(tick);
      size_t processed=0;

      while (this->_queue.next_event_time()<=tick) {
        auto trans = this->_queue.pop_and_get();
        _state_store.insert(trans->record(), trans->offset());
        ++(this->_processed_count);
        ++processed;
        this->send_to_sinks(trans);
      }

      // TODO is this expensive??
      _state_store_count.set(_state_store.aprox_size());
      return processed;
    }

    void garbage_collect(int64_t tick) override {
      _state_store.garbage_collect(tick);
    }

    void garbage_collect_one(int64_t tick) {
      _state_store.garbage_collect_one(tick);
    }

    int64_t offset() const {
      return _state_store.offset();
    }

    size_t queue_size() const override {
      return event_consumer<K, V>::queue_size();
    }

    int64_t next_event_time() const override {
      return event_consumer<K, V>::next_event_time();
    }

    std::shared_ptr<const krecord <K, V>> get(const K &key) const override {
      return _state_store.get(key);
    }

    typename kspp::materialized_source<K, V>::iterator begin(void) const override {
      return _state_store.begin();
    }

    typename kspp::materialized_source<K, V>::iterator end() const override {
      return _state_store.end();
    }

  private:
    std::shared_ptr<kspp::partition_source<K, V>> _source;
    STATE_STORE<K, V, CODEC> _state_store;
    metric_gauge     _state_store_count;
  };
}
