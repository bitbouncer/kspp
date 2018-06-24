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
            : _event_time(ts), _key(k), _value(std::make_shared<V>(v)) {
    }

    krecord(const K &k, std::shared_ptr<const V> v, int64_t ts = milliseconds_since_epoch())
            : _event_time(ts), _key(k), _value(v) {
    }

    krecord(const K &k, std::nullptr_t nullp, int64_t ts = milliseconds_since_epoch())
            : _event_time(ts), _key(k), _value(nullptr) {
    }

    inline bool operator==(const krecord<K,V>& other) const
    {
      if (_event_time != other._event_time)
        return false;

      if (_key != other._key)
        return false;

      if (_value.get() == nullptr)
        if (other._value.get() == nullptr)
          return true;
      else
          return false;

      return (*_value.get() == *other._value.get());
    }

    inline const K &key() const {
      return _key;
    }

    inline const V *value() const {
      return _value.get();
    }

    inline int64_t event_time() const {
      return _event_time;
    }

  private:
    const K _key;
    const std::shared_ptr<const V> _value;
    const int64_t _event_time;
  };

  template<class K, class V>
  inline std::shared_ptr<krecord<K,V>> make_krecord(const K &k, const V &v, int64_t ts = milliseconds_since_epoch()){
    return std::make_shared<krecord<K, V >>(k, v, ts);
  }

  template<class V>
  class krecord<void, V> {
  public:
    krecord(const V &v, int64_t ts = milliseconds_since_epoch())
            : _event_time(ts), _value(std::make_shared<V>(v)) {
    }

    krecord(std::shared_ptr<const V> v, int64_t ts = milliseconds_since_epoch())
            : _event_time(ts), _value(v) {
    }

    inline const V *value() const {
      return _value.get();
    }

    inline int64_t event_time() const {
      return _event_time;
    }

  private:
    const std::shared_ptr<const V> _value;
    const int64_t _event_time;
  };

  template<class V>
  inline std::shared_ptr<krecord<void,V>> make_krecord(const V &v, int64_t ts = milliseconds_since_epoch()){
    return std::make_shared<krecord<void, V >>(v, ts);
  }

  template<class K>
  class krecord<K, void> {
  public:
    krecord(const K &k, int64_t ts = milliseconds_since_epoch())
            : _event_time(ts), _key(k) {
    }

    inline const K &key() const {
      return _key;
    }

    inline int64_t event_time() const {
      return _event_time;
    }

  private:
    const K _key;
    const int64_t _event_time;
  };

  template<class K>
  inline std::shared_ptr<krecord<K, void>> make_krecord(const K &k, int64_t ts = milliseconds_since_epoch()){
    return std::make_shared<krecord<K, void >>(k, ts);
  }
}