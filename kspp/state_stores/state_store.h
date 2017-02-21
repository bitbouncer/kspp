#pragma once

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
  using sink_function = typename std::function<void(std::shared_ptr<krecord<K, V>>)>;

  virtual ~state_store() {}

  virtual void garbage_collect(int64_t tick) {}

  virtual void close() = 0;

  /**
  * Put or delete a record
  */
  virtual void insert(std::shared_ptr<krecord<K, V>> record) = 0;

  /**
  * commits the offset
  */
  virtual void commit(bool flush) = 0;
  
  /**
  * returns last offset
  */
  virtual int64_t offset() const = 0;

  virtual void start(int64_t offset) = 0;

  virtual size_t size() const = 0;

  // TBD really needed for counter store
  virtual void clear() = 0;

  void set_sink(sink_function f) {
    _sink = f;
  }

  /**
  * Returns a key-value pair with the given key
  */
  virtual std::shared_ptr<krecord<K, V>> get(const K& key) = 0;
  virtual typename kspp::materialized_source<K, V>::iterator begin() = 0;
  virtual typename kspp::materialized_source<K, V>::iterator end() = 0;

  protected:
  sink_function _sink;
};
};
