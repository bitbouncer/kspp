#include <type_traits>
#include <chrono>
#include <memory>
#include <cstdint>
#include <string>
#include <vector>
#include <mutex>
#include <thread>
#include <chrono>
#include <strstream>
#include <boost/uuid/uuid.hpp>
#include "metrics.h"
#include "type_name.h"

#pragma once
namespace kspp {
inline int64_t milliseconds_since_epoch() {
  return std::chrono::duration_cast<std::chrono::milliseconds>
    (std::chrono::system_clock::now().time_since_epoch()).count();
}

template<class K, class V>
struct krecord
{
  krecord() : event_time(-1), offset(-1) {}
  krecord(const K& k) : event_time(milliseconds_since_epoch()), offset(-1), key(k) {}
  krecord(const K& k, const V& v) : event_time(milliseconds_since_epoch()), offset(-1), key(k), value(std::make_shared<V>(v)) {}
  krecord(const K& k, std::shared_ptr<V> v) : event_time(milliseconds_since_epoch()), offset(-1), key(k), value(v) {}
  krecord(const K& k, std::shared_ptr<V> v, int64_t ts) : event_time(ts), offset(-1), key(k), value(v) {}

  K                  key;
  std::shared_ptr<V> value;
  int64_t            event_time;
  int64_t            offset;
};

template<class V>
struct krecord<void, V>
{
  krecord(const V& v) : event_time(milliseconds_since_epoch()), offset(-1), value(std::make_shared<V>(v)) {}
  krecord(std::shared_ptr<V> v) : event_time(milliseconds_since_epoch()), offset(-1), value(v) {}
  krecord(std::shared_ptr<V> v, int64_t ts) : event_time(ts), offset(-1), value(v) {}
  std::shared_ptr<V> value;
  int64_t            event_time;
  int64_t            offset;
};

template<class K>
struct krecord<K, void>
{
  krecord() : event_time(-1), offset(-1) {}
  krecord(const K& k) : event_time(milliseconds_since_epoch()), offset(-1), key(k) {}
  krecord(const K& k, int64_t ts) : event_time(ts), offset(-1), key(k) {}
  K                  key;
  int64_t            event_time;
  int64_t            offset;
};

class processor
{
  public:
  virtual ~processor() {}

  const std::vector<metric*>& get_metrics() const {
    return _metrics;
  }

  protected:
  // must be valid for processor lifetime  (cannot be removed)
  void add_metric(metric* p) { 
    _metrics.push_back(p);
  }

  //const size_t          _partition; or -1 if not valid??
  //partition_processor*  _upstream;
  std::vector<metric*>  _metrics;
};

class topic_processor : public processor
{
  public:
  virtual ~topic_processor() {}
  //virtual void init(processor_context*) {}
  virtual std::string name() const = 0;
  virtual std::string processor_name() const { return "partition_processor"; }

  virtual void poll(int timeout) {}
  virtual bool eof() const = 0;

  /**
  * Process an input record
  */
  virtual bool process_one() = 0;
  virtual void punctuate(int64_t timestamp) {}
  virtual void close() = 0;


  virtual void flush() {
    while (!eof())
      if (!process_one()) {
        //using namespace std::chrono_literals;
        //std::this_thread::sleep_for(10ms);
        ;
      }
    //if (_upstream)   TBD!!!!!
    //  _upstream->flush();
    while (!eof())
      if (!process_one()) {
        //using namespace std::chrono_literals;
        //std::this_thread::sleep_for(10ms);
        ;
      }
    punctuate(milliseconds_since_epoch());
  }
  
  protected:
};

class partition_processor : public processor
{
  public:
  virtual ~partition_processor() {}
  virtual std::string name() const = 0;
  virtual std::string processor_name() const { return "partition_processor"; }

  size_t depth() const {
    return _upstream ? _upstream->depth() + 1 : 0;
  }

  /**
  * Process an input record
  */
  virtual bool process_one() {
    return _upstream ? _upstream->process_one() : false;
  }

  virtual bool eof() const {
    return _upstream ? _upstream->eof() : true;
  }

  virtual void poll(int timeout) {}
  virtual void punctuate(int64_t timestamp) {}
  
  virtual void close()     {
    if (_upstream)
      _upstream->close();
  }

  inline uint32_t partition() const {
    return (uint32_t) _partition;
  }

  virtual void flush() {
    while (!eof())
      if (!process_one()) {
        //using namespace std::chrono_literals;
        //std::this_thread::sleep_for(10ms);
        ;
      }
    if (_upstream)
      _upstream->flush();
    while (!eof())
      if (!process_one()) {
        //using namespace std::chrono_literals;
        //std::this_thread::sleep_for(10ms);
        ;
      }
    punctuate(milliseconds_since_epoch());
  }

  virtual void start() {
    if (_upstream)
      _upstream->start();
  }

  virtual void start(int64_t offset) {
    if (_upstream)
      _upstream->start(offset);
  }

  virtual void commit() {}
  virtual void flush_offset() {}

  protected:
  partition_processor(partition_processor* upstream, size_t partition)
    : _upstream(upstream)
    , _partition(partition) {
  }

  const size_t          _partition;
  partition_processor*  _upstream;
};

template<class K, class V>
class partition_sink : public partition_processor
{
  public:
  typedef K key_type;
  typedef V value_type;
  typedef kspp::krecord<K, V> record_type;

  virtual int produce(std::shared_ptr<krecord<K, V>> r) = 0;
  inline int produce(const K& key, const V& value) {
    return produce(std::make_shared<krecord<K, V>>(key, value));
  }

  virtual size_t queue_len() = 0;
  protected:
  partition_sink(size_t partition)
    : partition_processor(NULL, partition) {
  }
};

// specialisation for void key
template<class V>
class partition_sink<void, V> : public partition_processor
{
  public:
  typedef void key_type;
  typedef V value_type;
  typedef kspp::krecord<void, V> record_type;

  virtual int produce(std::shared_ptr<krecord<void, V>> r) = 0;
  inline int produce(const V& value) {
    return produce(std::make_shared<krecord<void, V>>(value));
  }

  virtual size_t queue_len() = 0;
  protected:
  partition_sink(size_t partition)
    : partition_processor(NULL, partition) {
  }
};

// specialisation for void value
template<class K>
class partition_sink<K, void> : public partition_processor
{
  public:
  typedef K key_type;
  typedef void value_type;
  typedef kspp::krecord<K, void> record_type;

  virtual int produce(std::shared_ptr<krecord<K, void>> r) = 0;
  inline int produce(const K& key) {
    return produce(std::make_shared<krecord<K, void>>(key));
  }

  virtual size_t queue_len() = 0;
  protected:
  partition_sink(size_t partition)
    : partition_processor(NULL, partition) {}
};


inline uint32_t djb_hash(const char *str, size_t len) {
  uint32_t hash = 5381;
  for (size_t i = 0; i < len; i++)
    hash = ((hash << 5) + hash) + str[i];
  return hash;
}

template<class PK, class CODEC>
inline uint32_t get_partition_hash(const PK& key, std::shared_ptr<CODEC> codec) {
  enum { MAX_KEY_SIZE = 1000 };
  uint32_t partition_hash = 0;
  char key_buf[MAX_KEY_SIZE];
  size_t ksize = 0;
  std::strstream s(key_buf, MAX_KEY_SIZE);
  ksize = codec->encode(key, s);
  partition_hash = djb_hash(key_buf, ksize);
  return partition_hash;
}

template<class PK, class CODEC>
inline uint32_t get_partition(const PK& key, size_t nr_of_partitions, std::shared_ptr<CODEC> codec) {
  auto hash = get_partition_hash <PK, CODEC>(key, codec);
  return hash % nr_of_partitions;
}

template<class K, class V, class CODEC>
class topic_sink : public topic_processor
{
  public:
  typedef K key_type;
  typedef V value_type;
  typedef kspp::krecord<K, V> record_type;

  virtual int    produce(std::shared_ptr<krecord<K, V>> r) = 0;
  virtual size_t queue_len() = 0;
  virtual int produce(uint32_t partition_hash, std::shared_ptr<krecord<K, V>> r) = 0;
  inline  int produce(uint32_t partition_hash, const K& key, const V& value) {
    return produce(partition_hash, std::make_shared<krecord<K, V>>(key, value));
  }

  inline std::shared_ptr<CODEC> codec() {
    return _codec;
  }
  protected:
  topic_sink(std::shared_ptr<CODEC> codec)
    :_codec(codec) {}

  std::shared_ptr<CODEC> _codec;
};

// specialisation for void key
template<class V, class CODEC>
class topic_sink<void, V, CODEC> : public topic_processor
{
  public:
  typedef void key_type;
  typedef V value_type;
  typedef kspp::krecord<void, V> record_type;

  virtual int produce(std::shared_ptr<krecord<void, V>> r) = 0;
  virtual size_t queue_len() = 0;
  virtual int produce(uint32_t partition_hash, std::shared_ptr<krecord<void, V>> r) = 0;
  inline  int produce(uint32_t partition_hash, const V& value) {
    return produce(partition_hash, std::make_shared<krecord<void, V>>(value));
  }

  inline std::shared_ptr<CODEC> codec() {
    return _codec;
  }

  protected:
  topic_sink(std::shared_ptr<CODEC> codec)
    :_codec(codec) {}

  std::shared_ptr<CODEC> _codec;
};

// specialisation for void value
template<class K, class CODEC>
class topic_sink<K, void, CODEC> : public topic_processor
{
public:
  typedef K key_type;
  typedef void value_type;
  typedef kspp::krecord<K, void> record_type;

  virtual int produce(std::shared_ptr<krecord<K, void>> r) = 0;
  virtual size_t queue_len() = 0;
  virtual int produce(uint32_t partition_hash, std::shared_ptr<krecord<K, void>> r) = 0;
  inline  int produce(uint32_t partition_hash, const K& key) {
    return produce(partition_hash, std::make_shared<krecord<K, void>>(key));
  }

  inline std::shared_ptr<CODEC> codec() {
    return _codec;
  }

protected:
  topic_sink(std::shared_ptr<CODEC> codec)
    :_codec(codec) {}

  std::shared_ptr<CODEC> _codec;
};

template<class K, class V>
class partition_source : public partition_processor
{
  public:
  using sink_function = typename std::function<void(std::shared_ptr<krecord<K, V>>)>;

  partition_source(partition_processor* upstream, size_t partition)
    : partition_processor(upstream, partition) {
  }

  void add_sink(std::shared_ptr<partition_sink<K, V>> sink) {
    add_sink([sink](auto e) {
      sink->produce(e);
    });
  }

  template<class CODEC>
  void add_sink(std::shared_ptr<topic_sink<K, V, CODEC>> sink) {
    add_sink([sink](auto e) {
      sink->produce(e);
    });
  }

  void add_sink(sink_function sink) {
    _sinks.push_back(sink);
  }

  virtual bool is_dirty() = 0;

  protected:

  virtual void send_to_sinks(std::shared_ptr<krecord<K, V>> p) {
    if (!p)
      return;
    for (auto f : _sinks)
      f(p);
  }

  std::vector<sink_function> _sinks;
};

template<class K, class V>
class kmaterialized_source_iterator_impl
{
  public:
  virtual ~kmaterialized_source_iterator_impl() {}
  virtual void next() = 0;
  virtual std::shared_ptr<krecord<K, V>> item() const = 0;
  virtual bool valid() const = 0;
  virtual bool operator==(const kmaterialized_source_iterator_impl& other) const = 0;
  bool operator!=(const kmaterialized_source_iterator_impl& other) const { return !(*this == other); }
};

template<class K, class V>
class materialized_partition_source : public partition_source<K, V>
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
    std::shared_ptr<kmaterialized_source_iterator_impl<K, V>> _impl;
    public:
    explicit iterator(std::shared_ptr<kmaterialized_source_iterator_impl<K, V>> impl) : _impl(impl) {}
    iterator& operator++() { _impl->next(); return *this; }
    iterator operator++(int) { iterator retval = *this; ++(*this); return retval; }
    bool operator==(const iterator& other) const { return *_impl == *other._impl; }
    bool operator!=(const iterator& other) const { return !(*this == other); }
    std::shared_ptr<krecord<K, V>> operator*() const { return _impl->item(); }
  };
  virtual iterator begin() = 0;
  virtual iterator end() = 0;
  virtual std::shared_ptr<krecord<K, V>> get(const K& key) = 0;

  materialized_partition_source(partition_processor* upstream, size_t partition)
    : partition_source<K, V>(upstream, partition) {
  }
};

template<class K, class V>
class kstream_partition : public partition_source<K, V>
{
  public:
  kstream_partition(partition_processor* upstream, size_t partition)
    : partition_source<K, V>(upstream, partition) {
  }
};

template<class K, class V>
class ktable_partition : public materialized_partition_source<K, V>
{
  public:
  ktable_partition(partition_processor* upstream, size_t partition)
    : materialized_partition_source<K, V>(upstream, partition) {
  }
};
}; // namespace