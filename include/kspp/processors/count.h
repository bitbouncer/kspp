#include <functional>
#include <chrono>
#include <deque>
#include <kspp/kspp.h>
#pragma once

namespace kspp {
  template<class K, class V, template<typename, typename, typename> class STATE_STORE, class CODEC = void>
  class count_by_key : public event_consumer<K, void>, public materialized_source<K, V> {
  public:

    typedef K key_type;
    typedef V value_type;

    static constexpr const char* PROCESSOR_NAME = "count_by_key";

    template<typename... Args>
    count_by_key(std::shared_ptr<cluster_config> config,
                 std::shared_ptr<partition_source<K, void>> source,
    std::chrono::milliseconds punctuate_intervall, Args ... args)
    : event_consumer<K, void>()
    , materialized_source<K, V>(source.get(), source->partition())
    , stream_(source)
    , counter_store_(this->get_storage_path(config->get_storage_root()), args...)
    , punctuate_intervall_(punctuate_intervall.count()) // tbd we should use intervalls since epoch similar to windowed
    , next_punctuate_(0)
    , dirty_(false) {
      source->add_sink([this](auto e) { this->_queue.push_back(e); });
      this->add_metrics_label(KSPP_PROCESSOR_TYPE_TAG, PROCESSOR_NAME);
      this->add_metrics_label(KSPP_PARTITION_TAG, std::to_string(source->partition()));
    }

    ~count_by_key() {
      close();
    }

    std::string log_name() const override {
      return PROCESSOR_NAME;
    }

    void start(int64_t offset) override {
      if (offset != kspp::OFFSET_STORED) {
        counter_store_.clear(); // the completely erases the counters... only correct for BEGIN
      }
      stream_->start(offset);
    }

    void close() override {
      stream_->close();
    }

    size_t process(int64_t tick) override {
      stream_->process(tick);

      size_t processed=0;
      //forward up this timestamp
      while (this->_queue.next_event_time()<=tick){
        auto trans = this->_queue.pop_front_and_get();

        if (next_punctuate_ < trans->event_time()) {
          punctuate(next_punctuate_); // what happens here if message comes out of order??? TBD
          next_punctuate_ = trans->event_time() + punctuate_intervall_;
          dirty_ = false;
        }

        ++processed;
        ++(this->_processed_count);
        this->_lag.add_event_time(tick, trans->event_time());
        dirty_ = true; // aggregated but not committed
        counter_store_.insert(std::make_shared<krecord<K, V>>(trans->record()->key(), 1), trans->offset());
      }
      return processed;
    }

    void commit(bool flush) override {
      stream_->commit(flush);
    }

    size_t queue_size() const override {
      return event_consumer<K, void>::queue_size();
    }

    int64_t next_event_time() const override {
      return event_consumer<K, void>::next_event_time();
    }

    bool eof() const override {
      return ((queue_size() == 0) && stream_->eof());
    }

    /**
    take a snapshot of state and post it to sinks
    */
    void punctuate(int64_t timestamp) override{
      if (dirty_) { // keep event timestamps in counter store and only include the updated ones... TBD
        for (auto i : counter_store_) {
          //we need to create a new instance
          this->send_to_sinks(
              std::make_shared<kevent<K, V>>(std::make_shared<krecord<K, V>>(i->key(), *i->value(), timestamp)));
        }
      }
      dirty_ = false;
    }

    // inherited from kmaterialized_source
    std::shared_ptr<const krecord <K, V>> get(const K &key) const override{
      return counter_store_.get(key);
    }

    typename kspp::materialized_source<K, V>::iterator begin(void) const override {
      return counter_store_.begin();
    }

    typename kspp::materialized_source<K, V>::iterator end() const override {
      return counter_store_.end();
    }

  private:
    std::shared_ptr<partition_source < K, void>> stream_;
    STATE_STORE<K, V, CODEC> counter_store_;
    int64_t punctuate_intervall_;
    int64_t next_punctuate_;
    bool dirty_;
  };
}
