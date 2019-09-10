#include <chrono>
#include <memory>
#pragma once

namespace kspp {
  inline int64_t milliseconds_since_epoch() {
    return std::chrono::duration_cast<std::chrono::milliseconds>
        (std::chrono::system_clock::now().time_since_epoch()).count();
  }

  template<class K, class V>
  class krecord {
  public:
    krecord(const K &k, const V &v, int64_t ts = milliseconds_since_epoch())
        : event_time_(ts), key_(k), value_(std::make_shared<V>(v)) {
    }

    krecord(const K &k, std::shared_ptr<const V> v, int64_t ts = milliseconds_since_epoch())
        : event_time_(ts), key_(k), value_(v) {
    }

    krecord(const K &k, std::nullptr_t nullp, int64_t ts = milliseconds_since_epoch())
        : event_time_(ts), key_(k), value_(nullptr) {
    }

    krecord(const krecord& a)
        : event_time_(a.event_time_), key_(a.key_), value_(a.value_) {
    }

    inline bool operator==(const krecord<K,V>& other) const
    {
      if (event_time_ != other.event_time_)
        return false;

      if (key_ != other.key_)
        return false;

      if (value_.get() == nullptr)
        if (other.value_.get() == nullptr)
          return true;
        else
          return false;

      return (*value_.get() == *other.value_.get());
    }

    inline const K &key() const {
      return key_;
    }

    inline const V *value() const {
      return value_.get();
    }

    inline std::shared_ptr<const V> shared_value() const {
      return value_;
    }

    inline int64_t event_time() const {
      return event_time_;
    }

  private:
    const K key_;
    const std::shared_ptr<const V> value_;
    const int64_t event_time_;
  };


  template<class V>
  class krecord<void, V> {
  public:
    krecord(const V &v, int64_t ts = milliseconds_since_epoch())
        : event_time_(ts), value_(std::make_shared<V>(v)) {
    }

    krecord(std::shared_ptr<const V> v, int64_t ts = milliseconds_since_epoch())
        : event_time_(ts), value_(v) {
    }

    krecord(const krecord& a)
        : event_time_(a.event_time_), value_(a.value_) {
    }

    inline bool operator==(const krecord<void, V>& other) const
    {
      if (event_time_ != other.event_time_)
        return false;

      if (value_.get() == nullptr)
        if (other.value_.get() == nullptr)
          return true;
        else
          return false;

      return (*value_.get() == *other.value_.get());
    }


    inline const V *value() const {
      return value_.get();
    }

    inline std::shared_ptr<const V> shared_value() const {
      return value_;
    }

    inline int64_t event_time() const {
      return event_time_;
    }

  private:
    const std::shared_ptr<const V> value_;
    const int64_t event_time_;
  };


  template<class K>
  class krecord<K, void> {
  public:
    krecord(const K &k, int64_t ts = milliseconds_since_epoch())
        : event_time_(ts), key_(k) {
    }

    krecord(const krecord& a)
        : event_time_(a.event_time_), key_(a.key_) {
    }

    inline bool operator==(const krecord<K,void>& other) const
    {
      if (event_time_ != other.event_time_)
        return false;

      return key_ == other.key_;
    }

    inline const K &key() const {
      return key_;
    }

    inline int64_t event_time() const {
      return event_time_;
    }

  private:
    const K key_;
    const int64_t event_time_;
  };
}