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
    krecord(const K& k, const V& v) : event_time(-1), offset(-1), key(k), value(std::make_shared<V>(v)) {}
    krecord(const K& k, std::shared_ptr<V> v) : event_time(-1), offset(-1), key(k), value(v) {}

    K                  key;
    std::shared_ptr<V> value;
    int64_t            event_time;
    int64_t            offset;
  };
    
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
    virtual std::shared_ptr<krecord<K, V>> consume() = 0;
    virtual bool eof() const = 0;
    virtual void start() {}
    virtual void start(int64_t offset) {}
    virtual void commit() {}
    virtual void flush_offset() {}
  };

  template<class K, class V>
  class ksource_materialized: public ksource<K, V>
  {
    public:
    virtual std::shared_ptr<krecord<K, V>> get(const K& key) = 0;
  };
  
  template<class K, class V>
  class kstream : public ksource_materialized<K,V>
  {
  };
  
  template<class K, class V>
  class ktable_iterator_impl
  {
    public:
    virtual ~ktable_iterator_impl() {}
    virtual void next() = 0;
    virtual std::shared_ptr<krecord<K, V>> item() const = 0;
    virtual bool valid() const = 0;
    virtual bool operator==(const ktable_iterator_impl& other) const = 0;
    bool operator!=(const ktable_iterator_impl& other) const { return !(*this == other); }
  };

  template<class K, class V>
  class ktable : public ksource_materialized<K, V>
  {
    public:
    class iterator : public std::iterator <
      std::forward_iterator_tag,      // iterator_category
      std::shared_ptr<krecord<K, V>>, // value_type
      long,                           // difference_type
      std::shared_ptr<krecord<K, V>>*,// pointer
      std::shared_ptr<krecord<K, V>>  // reference
    >
    {
      std::shared_ptr<ktable_iterator_impl<K, V>> _impl;
      public:
      explicit iterator(std::shared_ptr<ktable_iterator_impl<K, V>> impl) : _impl(impl) {}
      iterator& operator++() { _impl->next(); return *this; }
      iterator operator++(int) { iterator retval = *this; ++(*this); return retval; }
      bool operator==(const iterator& other) const { return *_impl == *other._impl; }
      bool operator!=(const iterator& other) const { return !(*this == other); }
      reference operator*() const { return _impl->item(); }
    };
    virtual iterator begin() = 0;
    virtual iterator end() = 0;
  };

  template<class K, class V>
  class ksink : public knode
  {
    public:
    ksink() {}
    virtual int         produce(std::shared_ptr<krecord<K, V>> r) = 0;
    virtual size_t      queue_len() = 0;
    virtual std::string topic() const = 0;
    virtual void        poll(int timeout) = 0; // ????
  };

  template<class K, class V>
  std::shared_ptr<krecord<K, V>> create_krecord(const K& k, const V& v) {
    return std::make_shared<krecord<K, V>>(k, std::make_shared<V>(v));
  }

  template<class K, class V>
  std::shared_ptr<krecord<K, V>> create_krecord(const K& k) {
    return std::make_shared<krecord<K, V>>(k);
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