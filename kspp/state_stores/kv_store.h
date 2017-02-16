#include <kspp/kspp.h>
#include <string>
#include <cstdint>
#include <memory>
#pragma once

namespace kspp {
template<class K, class V>
class kv_store
{
  public:
    //kv_store(boost::filesystem::path storage_path, std::shared_ptr<CODEC> codec) {}

  virtual ~kv_store() {}

  //virtual std::string name() const = 0;

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
  
  //virtual void erase() = 0;

  /**
  * Returns a key-value pair with the given key
  */
  virtual std::shared_ptr<krecord<K, V>> get(const K& key) = 0;
  virtual typename kspp::materialized_source<K, V>::iterator begin() = 0;
  virtual typename kspp::materialized_source<K, V>::iterator end() = 0;
};
};
