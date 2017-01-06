#include <functional>
#include <deque>
#include <kspp/kspp_defs.h>
#include <kspp/state_stores/rocksdb_counter_store.h>
#pragma once

namespace csi {
  template<class K, class CODEC>
  class count_by_key : public materialized_partition_source<K, size_t>
  {
  public:
    count_by_key(std::shared_ptr<partition_source<K, void>> source, std::string storage_path, std::shared_ptr<CODEC> codec = std::make_shared<CODEC>())
      : materialized_partition_source(source->partition())
      , _stream(source)
      , _counter_store(name(), storage_path + "//" + name(), codec) {
      source->add_sink([this](auto e) {
        _queue.push_back(e);
      });
    }

    ~count_by_key() {
      close();
    }

    static std::shared_ptr<materialized_partition_source<K, size_t>> create(std::shared_ptr<partition_source<K, void>> source, std::string storage_path, std::shared_ptr<CODEC> codec = std::make_shared<CODEC>()) {
      return std::make_shared<count_by_key<K, CODEC>>(source, storage_path, codec);
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

    virtual typename csi::materialized_partition_source<K, size_t>::iterator begin(void) {
      return _counter_store.begin();
    }

    virtual typename csi::materialized_partition_source<K, size_t>::iterator end() {
      return _counter_store.end();
    }

  private:
    std::shared_ptr<partition_source<K, void>>      _stream;
    rocksdb_counter_store<K, CODEC>                 _counter_store;
    std::deque<std::shared_ptr<krecord<K, void>>>   _queue;
  };
}
