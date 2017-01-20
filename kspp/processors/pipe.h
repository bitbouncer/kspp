#include <kspp/kspp.h>
#pragma once

namespace kspp {
template<class K, class V>
class pipe : public partition_source<K, V>
{
  public:
  typedef K key_type;
  typedef V value_type;
  typedef kspp::krecord<K, V> record_type;

  pipe(size_t partition)
    : partition_source<K, V>(NULL, partition) {
  }

  pipe(std::shared_ptr<kspp::partition_source<K, V>> upstream, size_t partition)
    : partition_source<K, V>(upstream.get(), partition) {
    if (upstream)
      upstream->add_sink([this](auto r) {
      this->send_to_sinks(r);
    });
  }

  virtual std::string name() const {
    return "pipe";
  }

  virtual std::string processor_name() const {
    return "pipe";
  }

  virtual bool is_dirty() {
    return !(this->eof());
  }

  virtual int produce(std::shared_ptr<krecord<K, V>> r) {
    this->send_to_sinks(r);
    return 0;
  }

  inline int produce(const K& key, const V& value) {
    return produce(std::make_shared<krecord<K, V>>(key, value));
  }

  virtual size_t queue_len() {
    return 0;
  }
};

//<null, VALUE>
template<class V>
class pipe<void, V> : public partition_source<void, V>
{
  public:
  typedef void key_type;
  typedef V value_type;
  typedef kspp::krecord<void, V> record_type;

  pipe(size_t partition)
    : partition_source<void, V>(NULL, partition) {
  }

  pipe(std::shared_ptr<kspp::partition_source<void, V>> upstream, size_t partition)
    : partition_source<void, V>(upstream.get(), partition) {
    if (upstream)
      upstream->add_sink([this](auto r) {
      this->send_to_sinks(r);
    });
  }

  virtual std::string name() const {
    return "pipe";
  }

  virtual std::string processor_name() const {
    return "pipe";
  }

  virtual bool is_dirty() {
    return !(this->eof());
  }

  virtual int produce(std::shared_ptr<krecord<void, V>> r) {
    this->send_to_sinks(r);
    return 0;
  }

  inline int produce(const V& value) {
    return produce(std::make_shared<krecord<void, V>>(value));
  }

  virtual size_t queue_len() {
    return 0;
  }
};

template<class K>
class pipe<K, void> : public partition_source<K, void>
{
  public:
  typedef K key_type;
  typedef void value_type;
  typedef kspp::krecord<K, void> record_type;

  pipe(size_t partition)
    : partition_source<K, void>(NULL, partition) {
  }

  pipe(std::shared_ptr<kspp::partition_source<K, void>> upstream, size_t partition)
    : partition_source<K, void>(upstream.get(), partition) {
    if (upstream)
      upstream->add_sink([this](std::shared_ptr<krecord<K, void>> r) {
      this->send_to_sinks(r);
    });
  }

  virtual std::string name() const {
    return "pipe";
  }

  virtual std::string processor_name() const {
    return "pipe";
  }

  virtual bool is_dirty() {
    return !(this->eof());
  }
  
  virtual int produce(std::shared_ptr<krecord<K, void>> r) {
    this->send_to_sinks(r);
    return 0;
  }

  inline int produce(const K& key) {
    return produce(std::make_shared<krecord<K, void>>(key));
  }

  virtual size_t queue_len() {
    return 0;
  }
};
};