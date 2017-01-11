#include <functional>
#include <deque>
#include <kspp/kspp.h>
#include <kspp/state_stores/rocksdb_counter_store.h>
#pragma once

namespace kspp {
  template<class K, class CODEC>
  class count_by_key : public materialized_partition_source<K, size_t>
  {
  public:
    count_by_key(std::shared_ptr<partition_source<K, void>> source, std::string storage_path, int64_t punctuate_intervall, std::shared_ptr<CODEC> codec = std::make_shared<CODEC>())
      : materialized_partition_source(source->partition())
      , _stream(source)
      , _counter_store(name(), storage_path + "//" + name(), codec)
      , _punctuate_intervall(punctuate_intervall)
      , _next_punctuate(0)
      , _dirty(false) {
      source->add_sink([this](auto e) {
        _queue.push_back(e);
      });
    }

    ~count_by_key() {
      close();
    }

    static std::shared_ptr<materialized_partition_source<K, size_t>> create(std::shared_ptr<partition_source<K, void>> source, std::string storage_path, int64_t punctuate_intervall, std::shared_ptr<CODEC> codec = std::make_shared<CODEC>()) {
      return std::make_shared<count_by_key<K, CODEC>>(source, storage_path, punctuate_intervall, codec);
    }

    std::string name() const {
      return _stream->name() + "(count_by_key)_" + std::to_string(materialized_partition_source::partition());
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

        _dirty = true; // aggregated but not committed
        _queue.pop_front();
        _counter_store.add(e->key, 1);
      }
      return processed;
    }

    virtual void commit() {
      _stream->commit();
    }

    virtual bool eof() const {
      return _queue.size() == 0 && _stream->eof();
    }

    virtual bool is_dirty() {
      return (_dirty || !eof());
    }

    virtual void flush() {
      while (!eof())
        process_one();
      _stream->flush();
      while (!eof())
        process_one();
      punctuate(milliseconds_since_epoch());
    }

    /**
    take a snapshot of state and post it to sinks
    */
    virtual void punctuate(int64_t timestamp) {
      for (auto i : _counter_store) {
        i->event_time = timestamp;
        send_to_sinks(i);
      }
    }

    // inherited from kmaterialized_source
    virtual std::shared_ptr<krecord<K, size_t>> get(const K& key) {
      auto count = _counter_store.get(key);
      if (count)
        return std::make_shared<krecord<K, size_t>>(key, count);
      else
        return NULL;
    }

    virtual typename kspp::materialized_partition_source<K, size_t>::iterator begin(void) {
      return _counter_store.begin();
    }

    virtual typename kspp::materialized_partition_source<K, size_t>::iterator end() {
      return _counter_store.end();
    }

  private:
    std::shared_ptr<partition_source<K, void>>      _stream;
    rocksdb_counter_store<K, CODEC>                 _counter_store;
    std::deque<std::shared_ptr<krecord<K, void>>>   _queue;
    int64_t                                         _punctuate_intervall;
    int64_t                                         _next_punctuate;
    bool                                            _dirty;
  };
}
