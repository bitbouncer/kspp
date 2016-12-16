#include <functional>
#include <deque>
//#include <boost/log/trivial.hpp>
#include <kspp/kspp_defs.h>
#include <kspp/state_stores/counter_store.h>
#pragma once

namespace csi {
template<class K, class CODEC>
class count_keys : public kmaterialized_source<K, size_t>
{
  public:
  count_keys(std::shared_ptr<ksource<K, void>> source, std::string storage_path, std::shared_ptr<CODEC> codec = std::make_shared<CODEC>()) :
    _stream(source),
    _counter_store(name(), storage_path + "//" + name(), codec)
    {}

  ~count_keys() {
    close();
  }

  std::string name() const {
    return "count_keys-" + _stream->name();
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

  virtual std::shared_ptr<krecord<K, size_t>> consume() {
    if (_queue.size()) {
      auto p = *_queue.begin();
      _queue.pop_front();
      return p;
    }

    auto e = _stream->consume();
    if (!e)
      return NULL;

   _counter_store.add(e->key, 1);
   size_t count = _counter_store.get(e->key);
   return std::make_shared<krecord<K, size_t>>(e->key, count);
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

  virtual typename csi::kmaterialized_source<K, size_t>::iterator begin(void) {
    return _counter_store.begin();
  }

  virtual typename csi::kmaterialized_source<K, size_t>::iterator end() {
    return _counter_store.end();
  }

  private:
  std::shared_ptr<ksource<K, void>>               _stream;
  kkeycounter_store<K, CODEC>                     _counter_store;
  std::deque<std::shared_ptr<krecord<K, size_t>>> _queue;
};

}
