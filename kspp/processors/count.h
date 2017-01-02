#include <functional>
#include <deque>
#include <kspp/kspp_defs.h>
#include <kspp/state_stores/rocksdb_counter_store.h>
#pragma once

namespace csi {
template<class K, class CODEC>
//class count_partition_keys : public partition_sink<K, void>, materialized_partition_source<K, size_t>
class count_partition_keys : public materialized_partition_source<K, size_t>
{
  public:
    count_partition_keys(std::shared_ptr<partition_source<K, void>> source, std::string storage_path, std::shared_ptr<CODEC> codec = std::make_shared<CODEC>())
    : materialized_partition_source(source->partition())
    //, partition_sink(source->partition())
    , _stream(source)
    , _counter_store(name(), storage_path + "//" + name(), codec) {
      source->add_sink([this](auto e) {
        _queue.push_back(e);
      });
  }

  ~count_partition_keys() {
    close();
  }

  std::string name() const {
    return _stream->name() + "(count_partition_keys)_" + std::to_string(materialized_partition_source::partition());
  }

  virtual void start() {
    _stream->start();
  }

  virtual void start(int64_t offset) {
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

  virtual void poll(int timeout) {
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
    return _queue.size() ==0 && _stream->eof();
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
