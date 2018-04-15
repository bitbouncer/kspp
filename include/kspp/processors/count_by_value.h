#include <functional>
#include <chrono>
#include <deque>
#include <kspp/kspp.h>
#include "../kspp.h"

#pragma once

namespace kspp {
  template<class K, class V, template<typename, typename, typename> class STATE_STORE, class CODEC = void>
  class count_by_value : public materialized_source<K, V> {
    static constexpr const char* PROCESSOR_NAME = "count_by_value";
  public:
    template<typename... Args>
    count_by_value(topology_base &topology, std::shared_ptr <partition_source<K, V>> source,
                   std::chrono::milliseconds punctuate_intervall, Args... args)
            : materialized_source<K, V>(source.get(), source->partition()), _stream(source),
            _counter_store(this->get_storage_path(topology.get_storage_path()), args...), _punctuate_intervall(
                    punctuate_intervall.count()) // tbd we should use intervalls since epoch similar to windowed
              , _next_punctuate(0), _dirty(false), _in_count("in_count"), _lag() {
      source->add_sink([this](auto e) { this->_queue.push_back(e); });
      this->add_metrics_tag(KSPP_PROCESSOR_TYPE_TAG, "count_by_value");
      this->add_metrics_tag(KSPP_PARTITION_TAG, std::to_string(source->partition()));
    }

    ~count_by_value() {
      close();
    }

    std::string log_name() const override {
      return PROCESSOR_NAME;
    }

    void start(int64_t offset) override {
      if (offset != kspp::OFFSET_STORED)
        _counter_store.clear(); // the completely erases the counters...
      _stream->start(offset);
    }

    void close() override {
      _stream->close();
    }

    size_t process(int64_t tick) override {
      _stream->process(tick);
      size_t processed = 0;

      size_t processed=0;
      //forward up this timestamp
      while (this->_queue.next_event_time()<=tick){
        auto trans = this->_queue.pop_and_get();
        // should this be on processing time our message time???
        // what happens at end of stream if on messaage time...
        if (_next_punctuate < trans->event_time()) {
          punctuate(_next_punctuate); // what happens here if message comes out of order??? TBD
          //_next_punctuate = _next_punctuate + _punctuate_intervall;
          _next_punctuate = trans->event_time() + _punctuate_intervall;
          _dirty = false;
        }

        ++(this->_processed_count);
        ++processed;
        this->_lag.add_event_time(tick, trans->event_time());
        _dirty = true; // aggregated but not committed
        _counter_store.insert(trans->record(), trans->offset());
      }

      //TBD move this to generic punktuate if world time is large enough - REALLY READ the KIP about punctuate first...
      if (_next_punctuate < tick) {
        punctuate(_next_punctuate); // what happens here if message comes out of order??? TBD
        //_next_punctuate = _next_punctuate + _punctuate_intervall;
        _next_punctuate = tick + _punctuate_intervall;
        _dirty = false;
      }

      return processed;
    }

    void commit(bool flush) override {
      _stream->commit(flush);
    }

    size_t queue_size() const override {
      return event_consumer<K, V>::queue_size();
    }

    int64_t next_event_time() const override {
      return event_consumer<K, V>::next_event_time();
    }

    bool eof() const override {
      return queue_size() == 0 && _stream->eof();
    }

    /**
    take a snapshot of state and post it to sinks
    */
    void punctuate(int64_t timestamp) override {
      //if (_dirty) { // keep event timestamts in counter store and only include the updated ones... TBD
      for (auto i : _counter_store) {
        i->event_time = timestamp;
        this->send_to_sinks(std::make_shared < kevent < K, V >> (i));
      }
      //}
      _dirty = false;
    }

    // inherited from kmaterialized_source
    std::shared_ptr<const krecord <K, V>> get(const K &key) const override {
      return _counter_store.get(key);
    }

    typename kspp::materialized_source<K, V>::iterator begin(void) const override {
      return _counter_store.begin();
    }

    typename kspp::materialized_source<K, V>::iterator end() const override {
      return _counter_store.end();
    }

  private:
    std::shared_ptr <partition_source<K, V>> _stream;
    STATE_STORE<K, V, CODEC> _counter_store;
    int64_t _punctuate_intervall;
    int64_t _next_punctuate;
    bool _dirty;
  };
}
