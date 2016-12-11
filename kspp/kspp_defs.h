#include <memory>
#include <cstdint>
#include <string>

#pragma once
namespace csi {
  template<class K, class V>
  struct krecord
  {
    krecord() : event_time(-1), offset(-1) {}
    krecord(const K& k) : event_time(-1), offset(-1), key(k) {}
    krecord(const K& k, const V& v) : event_time(-1), offset(-1), key(k), value(new V(v)) {}
    //krecord(const K& k, std::unique_ptr<V> v) : event_time(-1), offset(-1), key(k), value(std::move(v)) {}

    K                  key;
    std::unique_ptr<V> value;
    int64_t            event_time;
    int64_t            offset;
  };

  /*
  template<class K, class V>
  class kprocessor
  {
  public:

  };
  */

  class knode
  {
  public:
    virtual ~knode() {}
    virtual void close() = 0;
  protected:
    knode() {}
  };

  template<class K, class V>
  class ksource : public knode
  {
  public:
    virtual std::unique_ptr<krecord<K, V>> consume() = 0;
    virtual bool eof() const = 0;
    virtual void start() {}
    virtual void start(int64_t offset) {}
    virtual void commit() {}
    virtual void flush_offset() {}
  };

  template<class K, class V>
  class ktable_iterator
  {
  public:
    virtual ~ktable_iterator() {}
    virtual void next() = 0;
    virtual std::unique_ptr<krecord<K, V>> item() const = 0;
    virtual bool valid() const = 0;
  };
  template<class K, class V>
  class ksource_materialized: public ksource<K, V>
  {
    public:
    virtual std::unique_ptr<krecord<K, V>> get(const K& key) = 0;
  };
  
  template<class K, class V>
  class kstream : public ksource_materialized<K,V>
  {
  };

  template<class K, class V>
  class ktable : public ksource_materialized<K, V>
  {
  public:
    virtual std::shared_ptr<csi::ktable_iterator<K,V>> iterator() = 0;
  };

  template<class K, class V>
  class ksink : public knode
  {
    public:
    ksink() {}
    virtual int         produce(std::unique_ptr<krecord<K, V>> r) = 0;
    virtual size_t      queue_len() = 0;
    virtual std::string topic() const = 0;
    virtual void        poll(int timeout) = 0; // ????
  };



  template<class K, class V>
  std::unique_ptr<krecord<K, V>> create_krecord(const K& k, const V& v) {
    return std::unique_ptr<krecord<K, V>>(new krecord<K, V>(k, v));
  }

  template<class K, class V>
  std::unique_ptr<krecord<K, V>> create_krecord(const K& k) {
    return std::unique_ptr<krecord<K, V>>(new krecord<K, V>(k));
  }

  template<class K, class V>
  size_t consume(ksource<K, V>& src, ksink<K, V>& dst) {
    auto p = src.consume();
    if (!p)
      return 0;
    dst.produce(std::move(p));
    return 1;
  }

  template<class K, class V>
  int produce(ksink<K, V>& sink, const K& key, const V& val) {
    return sink.produce(std::move<>(create_krecord<K, V>(key, val)));
  }

  template<class K, class V>
  int produce(ksink<K, V>& sink, const K& key) {
    return sink.produce(std::move<>(create_krecord<K, V>(key)));
  }

}; // namespace