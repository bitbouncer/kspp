#pragma once
namespace kspp {
  template<class K, class V>
  class counter_store
  {
  public:
    virtual ~counter_store() {}

    virtual void close() = 0;
    /**
    * Adds count to a counter
    */
    virtual void add(const K& key, V count) = 0;
    /**
    * Deletes a counter
    */
    virtual void del(const K& key) = 0;
    /**
    * Returns the counter for the given key
    */

    /**
    * erases all counters
    */
    virtual void erase() = 0;

    virtual typename kspp::materialized_source<K, V>::iterator begin(void) = 0;
    virtual typename kspp::materialized_source<K, V>::iterator end() = 0;
  };
};