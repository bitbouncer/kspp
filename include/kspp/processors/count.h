#include <functional>
#include <chrono>
#include <deque>
#include <kspp/kspp.h>

#pragma once

namespace kspp {
  template<class K, class V, template<typename, typename, typename> class STATE_STORE, class CODEC = void>
  class count_by_key : public event_consumer<K, void>, public materialized_source<K, V> {
  public:
    template<typename... Args>
    count_by_key(topology_base &topology,
                 std::shared_ptr<partition_source<K, void>> source,
    std::chrono::milliseconds punctuate_intervall, Args ... args)
    : event_consumer<K, void>()
    , materialized_source<K, V>(source.get(), source->partition())
    , _stream(source)
    , _counter_store(this->get_storage_path(topology.get_storage_path()), args...)
    , _punctuate_intervall(punctuate_intervall.count()) // tbd we should use intervalls since epoch similar to windowed
    , _next_punctuate(0)
    , _dirty(false)
    , _in_count("in_count")
    , _lag() {
      source->add_sink([this](auto e) {
        this->_queue.push_back(e);
      });
      this->add_metric(&_in_count);
      this->add_metric(&_lag);
    }

    ~count_by_key() {
      close();
    }

    virtual std::string simple_name() const {
      return "count_by_key";
    }

    virtual void start() {
      _stream->start();
    }

    virtual void start(int64_t offset) {
      _counter_store.clear(); // the completely erases the counters... only valid for -2...
      _stream->start(offset);
    }

    virtual void close() {
      _stream->close();
    }

    /*
    virtual int produce(std::shared_ptr<kevent<K, void>> r) {
      _queue.push_back(r);
      return 0;
    }
    */

    virtual bool process_one(int64_t tick) {
      _stream->process_one(tick);
      bool processed = (this->_queue.size() > 0);
      while (this->_queue.size()) {
        auto trans = this->_queue.front();
        // should this be on processing time our message time???
        // what happens at end of stream if on messaage time...
        if (_next_punctuate < trans->event_time()) {
          punctuate(_next_punctuate); // what happens here if message comes out of order??? TBD
          _next_punctuate = trans->event_time() + _punctuate_intervall;
          _dirty = false;
        }

        ++_in_count;
        _lag.add_event_time(tick, trans->event_time());
        _dirty = true; // aggregated but not committed
        this->_queue.pop_front();
        _counter_store.insert(std::make_shared<krecord<K, V>>(trans->record()->key(), 1), trans->offset());
      }
      return processed;
    }

    virtual void commit(bool flush) {
      _stream->commit(flush);
    }

    virtual size_t queue_len() const {
      return event_consumer < K, void > ::queue_len();
    }

    virtual bool eof() const {
      return queue_len() == 0 && _stream->eof();
    }

    /**
    take a snapshot of state and post it to sinks
    */
    virtual void punctuate(int64_t timestamp) {
      if (_dirty) { // keep event timestamps in counter store and only include the updated ones... TBD
        for (auto i : _counter_store) {
          //we need to create a new instance
          this->send_to_sinks(
                  std::make_shared<kevent<K, V>>(std::make_shared<krecord<K, V>>(i->key(), *i->value(), timestamp)));
        }
      }
      _dirty = false;
    }

    // inherited from kmaterialized_source
    virtual std::shared_ptr<const krecord <K, V>> get(const K &key) const {
      return _counter_store.get(key);
    }

    virtual typename kspp::materialized_source<K, V>::iterator begin(void) const {
      return _counter_store.begin();
    }

    virtual typename kspp::materialized_source<K, V>::iterator end() const {
      return _counter_store.end();
    }

  private:
    std::shared_ptr<partition_source < K, void>> _stream;
    STATE_STORE<K, V, CODEC> _counter_store;
    int64_t _punctuate_intervall;
    int64_t _next_punctuate;
    bool _dirty;
    metric_counter _in_count;
    metric_lag _lag;
  };
}
