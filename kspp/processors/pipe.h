#include <kspp/kspp.h>
#pragma once

namespace kspp {
template<class K, class V>
class pipe : public partition_source<K, V>
{
  public:
  typedef K key_type;
  typedef V value_type;
  typedef kspp::ktransaction<K, V> record_type;

  pipe(topology_base& topology)
    : partition_source<K, V>(nullptr, topology.partition()) {
  }

  pipe(topology_base& topology, std::shared_ptr<kspp::partition_source<K, V>> upstream)
    : partition_source<K, V>(upstream.get(), upstream->partition()) {
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

  virtual bool process_one(int64_t tick) {
    return this->_upstream ? this->_upstream->process_one(tick) : false;
  }

  virtual int produce(std::shared_ptr<ktransaction<K, V>> r) {
    this->send_to_sinks(r);
    return 0;
  }

  inline int produce(const K& key, const V& value) {
    return produce(std::make_shared<ktransaction<K, V>>(std::make_shared<krecord<K, V>>(key, value)));
  }

  virtual size_t queue_len() {
    return 0;
  }

  virtual void commit(bool force) {
    if (this->_upstream)
      this->_upstream->commit(force);
  }
};

//<null, VALUE>
template<class V>
class pipe<void, V> : public partition_source<void, V>
{
  public:
  typedef void key_type;
  typedef V value_type;
  typedef kspp::ktransaction<void, V> record_type;

  pipe(topology_base& topology, size_t partition)
    : partition_source<void, V>(nullptr, topology.partition()) {
  }

  pipe(topology_base& topology, std::shared_ptr<kspp::partition_source<void, V>> upstream)
    : partition_source<void, V>(upstream.get(), upstream->partition()) {
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

  virtual bool process_one(int64_t tick) {
    return this->_upstream ? this->_upstream->process_one(tick) : false;
  }

  virtual int produce(std::shared_ptr<ktransaction<void, V>> r) {
    this->send_to_sinks(r);
    return 0;
  }

  inline int produce(const V& value) {
    return produce(std::make_shared<ktransaction<void, V>>(std::make_shared<krecord<void, V>>(value)));
  }

  virtual size_t queue_len() {
    return 0;
  }

  virtual void commit(bool force) {
    if (this->_upstream)
      this->_upstream->commit(force);
  }
};

template<class K>
class pipe<K, void> : public partition_source<K, void>
{
  public:
  typedef K key_type;
  typedef void value_type;
  typedef kspp::ktransaction<K, void> record_type;

  pipe(topology_base& topology)
    : partition_source<K, void>(nullptr, topology.partition()) {
  }
  
  pipe(topology_base& topology, std::shared_ptr<kspp::partition_source<K, void>> upstream)
    : partition_source<K, void>(upstream.get(), upstream->partition()) {
    if (upstream)
      upstream->add_sink([this](std::shared_ptr<ktransaction<K, void>> r) {
      this->send_to_sinks(r);
    });
  }

  virtual std::string name() const {
    return "pipe";
  }

  virtual std::string processor_name() const {
    return "pipe";
  }

  virtual bool process_one(int64_t tick) {
    return this->_upstream ? this->_upstream->process_one(tick) : false;
  }

  virtual int produce(std::shared_ptr<ktransaction<K, void>> r) {
    this->send_to_sinks(r);
    return 0;
  }

  inline int produce(const K& key) {
    return produce(std::make_shared<ktransaction<K, void>>(std::make_shared<krecord<K, void>>(key)));
  }

  virtual size_t queue_len() {
    return 0;
  }
  
  virtual void commit(bool force) {
    if (this->_upstream)
      this->_upstream->commit(force);
  }
};
};