#include <kspp/internal/event_queue.h>
#include <memory>
#pragma once

namespace kspp {
  template<class K, class V>
  class event_consumer {
  public:
    typedef K key_type;
    typedef V value_type;

    event_consumer() {
    }

    inline std::string key_type_name() const {
      return type_name<K>::get();
    }

    inline std::string value_type_name() const {
      return type_name<V>::get();
    }

    inline size_t queue_size() const {
      return _queue.size();
    }

    inline int64_t next_event_time() const {
      return this->_queue.next_event_time();
    }

    inline void push_back(std::shared_ptr<krecord<K, V>> r) {
      this->_queue.push_back(std::make_shared<kevent<K, V>>(r));
    }

    inline void push_back(std::shared_ptr <kevent<K, V>> ev) {
      this->_queue.push_back(ev);
    }

    inline void push_back(const K &key, const V &value, int64_t ts = milliseconds_since_epoch()) {
      this->_queue.push_back(std::make_shared<kevent<K,V>>(std::make_shared < krecord < K, V >> (key, value, ts)));
    }


    //produce with custom partition hash

    inline void push_back(uint32_t partition_hash, std::shared_ptr<krecord<K, V>> r) {
      if (r) {
        auto ev = std::make_shared<kevent<K, V>>(r, nullptr, partition_hash);
        this->_queue.push_back(ev);
      }
    }

    inline void push_back(uint32_t partition_hash, std::shared_ptr<kevent<K, V>> t) {
      if (t) {
        auto ev2 = std::make_shared<kevent<K, V>>(t->record(),
            t->id(),
            partition_hash); // make new one since change the partition
        this->_queue.push_back(ev2);
      }
    }

    inline void
    push_back(uint32_t partition_hash, const K &key, const V &value, int64_t ts = milliseconds_since_epoch()) {
      push_back(partition_hash, std::make_shared<krecord<K, V>>(key, value, ts));
    }

  protected:
    kspp::event_queue<K, V> _queue;
  };

// specialisation for void key
  template<class V>
  class event_consumer<void, V> {
  public:
    typedef void key_type;
    typedef V value_type;


    event_consumer() {
    }

    inline std::string key_type_name() const {
      return "void";
    }

    inline std::string value_type_name() const {
      return type_name<V>::get();
    }

    inline size_t queue_size() const {
      return _queue.size();
    }

    inline int64_t next_event_time() const {
      return this->_queue.next_event_time();
    }

    inline void push_back(std::shared_ptr <krecord<void, V>> r) {
      this->_queue.push_back(std::make_shared < kevent < void, V >> (r));
    }

    inline void push_back(std::shared_ptr <kevent<void, V>> ev) {
      this->_queue.push_back(ev);
    }

    inline void push_back(const V &value, int64_t ts = milliseconds_since_epoch()) {
      this->_queue.push_back(std::make_shared < kevent < void,
                             V >> (std::make_shared < krecord < void, V >> (value, ts)));
    }


    inline void push_back(uint32_t partition_hash, std::shared_ptr<krecord<void, V>> r) {
      if (r) {
        auto ev = std::make_shared<kevent<void, V>>(r);
        ev->_partition_hash = partition_hash;
        this->_queue.push_back(ev);
      }
    }

    inline void push_back(uint32_t partition_hash, std::shared_ptr<kevent<void, V>> ev) {
      if (ev) {
        auto ev2 = std::make_shared<kevent<void, V>>(*ev); // make new one since change the partition
        ev2->_partition_hash = partition_hash;
        this->_queue.push_back(ev2);
      }
    }

    inline void push_back(uint32_t partition_hash, const V &value, int64_t ts = milliseconds_since_epoch()) {
      push_back(partition_hash, std::make_shared<krecord<void, V>>(value, ts));
    }

  protected:
    kspp::event_queue<void, V> _queue;
  };

// specialisation for void value
  template<class K>
  class event_consumer<K, void> {
  public:
    typedef K key_type;
    typedef void value_type;

    inline std::string key_type_name() const {
      return type_name<K>::get();
    }

    inline std::string value_type_name() const {
      return "void";
    }

    inline size_t queue_size() const {
      return _queue.size();
    }

    inline int64_t next_event_time() const {
      return this->_queue.next_event_time();
    }

    inline void push_back(std::shared_ptr <krecord<K, void>> r) {
      this->_queue.push_back(std::make_shared < kevent < K, void >> (r));
    }

    inline void push_back(std::shared_ptr <kevent<K, void>> ev) {
      this->_queue.push_back(ev);
    }

    inline void push_back(const K &key, int64_t ts = milliseconds_since_epoch()) {
      this->_queue.push_back(std::make_shared < kevent < K,
                             void >> (std::make_shared < krecord < K, void >> (key, ts)));
    }

    inline void push_back(uint32_t partition_hash, std::shared_ptr<krecord<K, void>> r) {
      if (r) {
        auto ev = std::make_shared<kevent<K, void>>(r);
        ev->_partition_hash = partition_hash;
        this->_queue.push_back(ev);
      }
    }

    inline void push_back(uint32_t partition_hash, std::shared_ptr<kevent<K, void>> ev) {
      if (ev) {
        auto ev2 = std::make_shared<kevent<K, void>>(*ev); // make new one since change the partition
        ev2->_partition_hash = partition_hash;
        this->_queue.push_back(ev2);
      }
    }

    inline void push_back(uint32_t partition_hash, const K &key, int64_t ts = milliseconds_since_epoch()) {
      push_back(partition_hash, std::make_shared<krecord<K, void>>(key, ts));
    }

  protected:
    kspp::event_queue<K, void> _queue;
  };
}

template<class K, class V>
void insert(kspp::event_consumer<K, V>& eventConsumer, const kspp::krecord<K, V >& r){
  auto kr = std::make_shared<kspp::krecord<K, V >>(r);
  eventConsumer.push_back(kr);
}

template<class K, class V>
void insert(kspp::event_consumer<K, V>& eventConsumer, const K &k, const V &v, int64_t ts = kspp::milliseconds_since_epoch()){
  auto kr = std::make_shared<kspp::krecord<K, V >>(k, v, ts);
  eventConsumer.push_back(kr);
}

template<class K, class V>
void insert(kspp::event_consumer<K, V>& eventConsumer, const K &k, std::shared_ptr<const V> p, int64_t ts = kspp::milliseconds_since_epoch()){
  auto kr = std::make_shared<kspp::krecord<K, V >>(k, p, ts);
  eventConsumer.push_back(kr);
}

template<class K, class V>
void insert(kspp::event_consumer<K, V>& eventConsumer, const K &k, std::shared_ptr<V> p, int64_t ts = kspp::milliseconds_since_epoch()){
  auto kr = std::make_shared<kspp::krecord<K, V >>(k, p, ts);
  eventConsumer.push_back(kr);
}

template<class K, class V>
void erase(kspp::event_consumer<K, V>& eventConsumer, const K &k, int64_t ts = kspp::milliseconds_since_epoch()){
  auto kr = std::make_shared<kspp::krecord<K, V >>(k, nullptr, ts);
  eventConsumer.push_back(kr);
}

template<class K>
void insert(kspp::event_consumer<K, void>& eventConsumer, const K &k, int64_t ts = kspp::milliseconds_since_epoch()){
  auto kr = std::make_shared<kspp::krecord<K, void>>(k, ts);
  eventConsumer.push_back(kr);
}

template<class V>
void insert(kspp::event_consumer<void, V>& eventConsumer, const V &v, int64_t ts = kspp::milliseconds_since_epoch()){
  auto kr = std::make_shared<kspp::krecord<void, V >>(v, ts);
  eventConsumer.push_back(kr);
}

template<class V>
void insert(kspp::event_consumer<void, V>& eventConsumer, std::shared_ptr<const V> p, int64_t ts = kspp::milliseconds_since_epoch()){
  auto kr = std::make_shared<kspp::krecord<void, V >>(p, ts);
  eventConsumer.push_back(kr);
}