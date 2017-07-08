#pragma once

#include <kspp/krecord.h>
#include <kspp/kevent.h>
#include <kspp/kspp.h>
#include <string>
#include <cstdint>
#include <memory>
#pragma once

// this should inherit from a state-store base class...
namespace kspp {
template<class K, class V>
class state_store
{
  public:
  using sink_function = typename std::function<void(std::shared_ptr<kevent<K, V>>)>;

  virtual ~state_store() {}

  virtual void garbage_collect(int64_t tick) {}

  virtual void close() = 0;

  /**
  * Put or delete a record
  */
  inline void insert(std::shared_ptr<const krecord<K, V>> record, int64_t offset) {
    _insert(record, offset);
  }

  /**
  * commits the offset
  */
  virtual void commit(bool flush) = 0;
  
  /**
  * returns last offset
  */
  virtual int64_t offset() const = 0;

  virtual void start(int64_t offset) = 0;

  virtual size_t aprox_size() const = 0;

  virtual size_t exact_size() const = 0;

  // TBD really needed for counter store
  virtual void clear() = 0;

  void set_sink(sink_function f) {
    _sink = f;
  }

  /**
  * Returns a key-value pair with the given key
  */
  virtual std::shared_ptr<const krecord<K, V>> get(const K &key) const = 0;

  virtual typename kspp::materialized_source<K, V>::iterator begin() const = 0;

  virtual typename kspp::materialized_source<K, V>::iterator end() const = 0;

  protected:
  virtual void _insert(std::shared_ptr<const krecord<K, V>> record, int64_t offset) = 0;

  sink_function _sink;
};
};
