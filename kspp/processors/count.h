#include <functional>
#include <deque>
#include <kspp/kspp.h>
#include <kspp/state_stores/rocksdb_counter_store.h>
#pragma once

namespace kspp {
  template<class K, class V, class CODEC>
  class count_by_key : public materialized_partition_source<K, V>
  {
  public:
    count_by_key(std::shared_ptr<partition_source<K, void>> source, std::string storage_path, int64_t punctuate_intervall, std::shared_ptr<CODEC> codec = std::make_shared<CODEC>())
      : materialized_partition_source<K, V>(source.get(), source->partition())
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

    static std::shared_ptr<materialized_partition_source<K, V>> create(std::shared_ptr<partition_source<K, void>> source, std::string storage_path, int64_t punctuate_intervall, std::shared_ptr<CODEC> codec = std::make_shared<CODEC>()) {
      return std::make_shared<count_by_key<K, CODEC>>(source, storage_path, punctuate_intervall, codec);
    }

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
        return NULL;
      */
      return std::make_shared<krecord<K, V>>(key, count);
    }


    virtual typename kspp::materialized_partition_source<K, V>::iterator begin(void) {
      return _counter_store.begin();
    }

    virtual typename kspp::materialized_partition_source<K, V>::iterator end() {
      return _counter_store.end();
    }

  private:
    std::shared_ptr<partition_source<K, void>>      _stream;
    rocksdb_counter_store<K, V, CODEC>              _counter_store;
    std::deque<std::shared_ptr<krecord<K, void>>>   _queue;
    int64_t                                         _punctuate_intervall;
    int64_t                                         _next_punctuate;
    bool                                            _dirty;
  };
}
