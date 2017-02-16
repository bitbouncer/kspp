#include <functional>
#include <chrono>
#include <deque>
#include <kspp/kspp.h>
#include <kspp/impl/state_stores/rocksdb_counter_store.h>
#pragma once

namespace kspp {
  template<class K, class V, class CODEC>
  class count_by_key : public materialized_source<K, V>
  {
  public:
    count_by_key(topology_base& topology, std::shared_ptr<partition_source<K, void>> source, std::chrono::milliseconds punctuate_intervall, std::shared_ptr<CODEC> codec)
      : materialized_source<K, V>(source.get(), source->partition())
      , _stream(source)
      , _counter_store(name(), get_storage_path(topology.get_storage_path()), codec)
      , _punctuate_intervall(punctuate_intervall.count()) // tbd we should use intervalls since epoch similar to windowed 
      , _next_punctuate(0)
      , _dirty(false)
      , _records_count("total_count")
      , _lag() {
      source->add_sink([this](auto e) {
        _queue.push_back(e);
      });
      this->add_metric(&_records_count);
      this->add_metric(&_lag);
    }

    ~count_by_key() {
      close();
    }

    virtual std::string processor_name() const { return "count_by_key"; }

    std::string name() const {
      return _stream->name() + "-count_by_key()[" + type_name<K>::get() + ", " + type_name<V>::get() + "]";
    }

    virtual void start() {
      _stream->start();
    }

    virtual void start(int64_t offset) {
      _counter_store.erase(); // the completely erases the counters... only valid for -2...
      _stream->start(offset);
    }

    virtual void close() {
      _stream->close();
    }

    virtual int produce(std::shared_ptr<krecord<K, void>> r) {
      _queue.push_back(r);
      return 0;
    }

    virtual size_t queue_len() {
      return _queue.size();
    }

    virtual bool process_one() {
      _stream->process_one();
      bool processed = (_queue.size() > 0);
      while (_queue.size()) {
        auto e = _queue.front();
        // should this be on processing time our message time??? 
        // what happens at end of stream if on messaage time...
        if (_next_punctuate < e->event_time) {
          punctuate(_next_punctuate); // what happens here if message comes out of order??? TBD
          _next_punctuate = e->event_time + _punctuate_intervall;
          _dirty = false;
        }

        ++_records_count;
        _lag.add_event_time(e->event_time);
        _dirty = true; // aggregated but not committed
        _queue.pop_front();
        _counter_store.add(e->key, 1);
      }
      return processed;
    }

    virtual void commit(bool flush) {
      _stream->commit(flush);
    }

    virtual bool eof() const {
      return _queue.size() == 0 && _stream->eof();
    }

    /**
    take a snapshot of state and post it to sinks
    */
    virtual void punctuate(int64_t timestamp) {
      if (_dirty) { // keep event timestamts in counter store and only include the updated ones... TBD
        for (auto i : _counter_store) {
          i->event_time = timestamp;
          this->send_to_sinks(i);
        }
      }
      _dirty = false;
    }

    // inherited from kmaterialized_source
    virtual std::shared_ptr<krecord<K, V>> get(const K& key) {
      V count = (V) _counter_store.get(key); // TBD
      /*
      if (count)
        return std::make_shared<krecord<K, V>>(key, count);
      else
        return nullptr;
      */
      return std::make_shared<krecord<K, V>>(key, count);
    }


    virtual typename kspp::materialized_source<K, V>::iterator begin(void) {
      return _counter_store.begin();
    }

    virtual typename kspp::materialized_source<K, V>::iterator end() {
      return _counter_store.end();
    }

  private:
  boost::filesystem::path get_storage_path(boost::filesystem::path storage_path) {
    boost::filesystem::path p(storage_path);
    p /= sanitize_filename(name());
    return p;
  }

    std::shared_ptr<partition_source<K, void>>      _stream;
    rocksdb_counter_store<K, V, CODEC>              _counter_store;
    std::deque<std::shared_ptr<krecord<K, void>>>   _queue;
    int64_t                                         _punctuate_intervall;
    int64_t                                         _next_punctuate;
    bool                                            _dirty;
    metric_counter                                  _records_count;
    metric_lag                                      _lag;
  };
}
