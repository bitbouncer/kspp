#include <cstdint>
#include <kspp/kspp.h>
#include <iostream>
#pragma once

namespace kspp {
template<class K>
class token_bucket
{
  public:
  virtual ~token_bucket() {}

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
  //virtual typename kspp::materialized_partition_source<K, size_t>::iterator begin(void) = 0;
  //virtual typename kspp::materialized_partition_source<K, size_t>::iterator end() = 0;
};

template<class K>
class mem_token_bucket : public token_bucket<K>
{
  public:
  mem_token_bucket(int64_t agetime, size_t capacity)
    : token_bucket()
    , _config(agetime, capacity) {
  }
    
  virtual ~mem_token_bucket() {}

  virtual void close() {}

  /**
  * Adds count to bucket
  * returns true if bucket has capacity
  */
  virtual bool consume(const K& key, int64_t timestamp) {
    std::map<K, std::shared_ptr<bucket>>::iterator item = _buckets.find(key);
    if (item == _buckets.end()) {
      auto b = std::make_shared<bucket>(_config.capacity);
      bool res = b->consume_one(&_config, timestamp);
      _buckets[key] = b;
      return res;
    }
    return item->second->consume_one(&_config, timestamp);
  }
  /**
  * Deletes a counter
  */
  virtual void del(const K& key) {
    _buckets.erase(key);
  }
  
  /**
  * Returns the counter for the given key
  */

  /**
  * erases all counters
  */
  virtual void erase() {
    _buckets.clear();
  }

  //virtual typename kspp::materialized_partition_source<K, size_t>::iterator begin(void) {}
  //virtual typename kspp::materialized_partition_source<K, size_t>::iterator end() {}


  protected:

  struct config
  {
    config(int64_t filltime_, size_t capacity_)
      : filltime(filltime_)
      , capacity(capacity_)
      , min_tick(std::max<int64_t>(1, filltime_/capacity_))
      , fillrate_per_ms(((double) capacity)/filltime) {
    }
    
    int64_t filltime;
    size_t  capacity;
    int64_t min_tick;
    double  fillrate_per_ms;
  };

  class bucket
  {
    public:
    bucket(size_t capacity)
      : _tokens(capacity)
      , _tstamp(0) {
    }

    inline bool consume_one(const config* conf, int64_t ts) {
      __age(conf, ts);
      if (_tokens ==0)
        return false;
      _tokens -= 1;
      return true;
    }

    protected:
    void __age(const config* conf, int64_t ts) {
      auto delta_ts = ts - _tstamp;
      size_t delta_count = (size_t) (delta_ts * conf->fillrate_per_ms);
      if (delta_count) {
        _tstamp = ts;
        _tokens = std::min<size_t>(conf->capacity, _tokens + delta_count);
      }
    }

      //virtual typename kspp::materialized_partition_source<K, size_t>::iterator begin(void) = 0;
      //virtual typename kspp::materialized_partition_source<K, size_t>::iterator end() = 0;
      // global
    size_t  _tokens;
    int64_t _tstamp;
  }; // class bucket

  protected:
  const config                         _config;
  std::map<K, std::shared_ptr<bucket>> _buckets;
};

};