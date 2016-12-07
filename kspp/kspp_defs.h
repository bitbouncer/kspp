#include <memory>
#include <cstdint>

#pragma once
namespace csi {
  template<class K, class V>
  struct krecord
  {
    krecord() : event_time(-1), offset(-1) {}
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
  protected:
    knode() {}
  };

  template<class K, class V>
  class sink : public knode
  {
  public:
    sink() {}
    virtual int produce(const K& key, const V& val) = 0;
    virtual int produce(const K& key) = 0;
  };

  template<class K, class V>
  class ksource : public knode
  {
  public:
    virtual std::unique_ptr<krecord<K, V>> consume() = 0;
    std::unique_ptr<krecord<K, V>> get(const K& key) { return NULL; }
    virtual bool eof() const = 0;
    virtual void start() {}
    virtual void start(int64_t offset) {}
    virtual void close() {}
    virtual void commit() {}
    virtual void flush_offset() {}
  };
}; // namespace