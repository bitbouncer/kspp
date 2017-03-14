#include <map>
#include <chrono>
#include <cstdint>
#include <kspp/kspp.h>
#pragma once

namespace kspp {
template<class K>
class token_bucket_store
{
  public:
  virtual ~token_bucket_store() {}

  virtual void close() = 0;

  /**
  * Adds count to bucket
  * returns true if bucket has capacity
  */
  virtual bool consume(const K& key, int64_t timestamp) = 0;

  /**
  * Deletes a bucket
  */
  virtual void del(const K& key) = 0;


  /**
  * erases all counters
  */
  virtual void erase() = 0;

  // SHOULD BE IMPLEMENTED
  virtual size_t get(const K& key) = 0;
  //virtual typename kspp::materialized_source<K, size_t>::iterator begin(void) = 0;
  //virtual typename kspp::materialized_source<K, size_t>::iterator end() = 0;
};
};