#include <chrono>
#pragma once

namespace kspp {
inline int64_t milliseconds_since_epoch() {
  return std::chrono::duration_cast<std::chrono::milliseconds>
    (std::chrono::system_clock::now().time_since_epoch()).count();
}

template<class K, class V>
struct krecord
{
  krecord() 
    : event_time(-1) {
  }

  krecord(const K& k) 
    : event_time(milliseconds_since_epoch()), 
    key(k) {
  }

  krecord(const K& k, const V& v, int64_t ts = milliseconds_since_epoch()) 
    : event_time(ts)
    , key(k)
    , value(std::make_shared<V>(v)) {
  }

  krecord(const K& k, std::shared_ptr<V> v, int64_t ts = milliseconds_since_epoch())
    : event_time(ts),
    key(k), 
    value(v) {
  }
  
  krecord(const K& k, std::nullptr_t nullp, int64_t ts = milliseconds_since_epoch()) 
    : event_time(ts), 
    key(k), 
    value(nullptr) {
  }

  K                   key;
  std::shared_ptr<V>  value;
  const int64_t       event_time;
};

template<class V>
struct krecord<void, V>
{
  krecord() 
    : event_time(-1) {
  }
  
  krecord(const V& v, int64_t ts = milliseconds_since_epoch()) 
    : event_time(ts), 
    value(std::make_shared<V>(v)) {
  }

  krecord(std::shared_ptr<V> v, int64_t ts = milliseconds_since_epoch()) 
    : event_time(ts), 
    value(v) {
  }

  std::shared_ptr<V> value;
  const int64_t      event_time;
};

template<class K>
struct krecord<K, void>
{
  krecord() 
    : event_time(-1) {
  }

  krecord(const K& k, int64_t ts = milliseconds_since_epoch()) 
    : event_time(ts), 
    key(k) {
  }
  
  K             key;
  const int64_t event_time;
};
}