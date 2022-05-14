#include <map>
#include <chrono>
#include <cstdint>
#include <kspp/kspp.h>
#include "state_store.h"

#pragma once

namespace kspp {
  template<class K, class V>
  class mem_token_bucket_store : public state_store<K, V> {
  protected:
    struct config {
      config(int64_t filltime_, V capacity_)
          : filltime(filltime_), capacity(capacity_), min_tick(std::max<int64_t>(1, filltime_ / capacity_)),
            fillrate_per_ms(((double) capacity) / filltime) {}

      int64_t filltime;
      V capacity;
      int64_t min_tick;
      double fillrate_per_ms;
    };

    class bucket {
    public:
      bucket(V capacity)
          : _tokens(capacity), _tstamp(0) {}

      inline bool consume_one(const config *conf, int64_t ts) {
        __age(conf, ts);
        if (_tokens == 0)
          return false;
        _tokens -= 1;
        return true;
      }

      inline V token() const {
        return _tokens;
      }

      inline int64_t timestamp() const {
        return _tstamp;
      }

    protected:
      void __age(const config *conf, int64_t ts) {
        auto delta_ts = ts - _tstamp;
        int64_t delta_count = (int64_t) (delta_ts * conf->fillrate_per_ms);
        if (delta_count > 0) { // no ageing on negative deltas
          _tstamp = ts;
          _tokens = (V) std::min<int64_t>(conf->capacity, _tokens + delta_count);
        }
      }

      V _tokens;
      int64_t _tstamp;
    }; // class bucket

    class iterator_impl
        : public kmaterialized_source_iterator_impl<K, V> {
    public:
      enum seek_pos_e {
        BEGIN, END
      };

      iterator_impl(const std::map<K, std::shared_ptr<bucket>> &container, seek_pos_e pos)
          : _container(container), _it(pos == BEGIN ? _container.begin() : _container.end()) {}

      bool valid() const override {
        return _it != _container.end();
      }

      void next() override {
        if (_it == _container.end())
          return;
        ++_it;
      }

      std::shared_ptr<const krecord<K, V>> item() const override {
        if (_it == _container.end())
          return nullptr;
        return std::make_shared<kspp::krecord<K, V>>(_it->first, _it->second->token(), _it->second->timestamp());
      }

      bool operator==(const kmaterialized_source_iterator_impl<K, V> &other) const override {
        if (valid() && !other.valid())
          return false;
        if (!valid() && !other.valid())
          return true;
        if (valid() && other.valid())
          return _it->first == ((const iterator_impl &) other)._it->first;
        return false;
      }

    private:
      const std::map<K, std::shared_ptr<bucket>> &_container;
      typename std::map<K, std::shared_ptr<bucket>>::const_iterator _it;
    };

  public:
    mem_token_bucket_store(std::chrono::milliseconds agetime, V capacity)
        : state_store<K, V>(), _config(agetime.count(), capacity), current_offset_(-1) {
    }

    static std::string type_name() {
      return "mem_token_bucket_store";
    }

    void close() override {
    }

    /**
    * commits the offset
    */
    void commit(bool flush) override {
      // noop
    }

    /**
    * returns last offset
    */
    int64_t offset() const override {
      return current_offset_;
    }

    void start(int64_t offset) override {
      current_offset_ = offset;
    }

    /**
    * Adds count to bucket
    * returns true if bucket has capacity
    */
    bool consume(const K &key, int64_t timestamp) {
      typename std::map<K, std::shared_ptr<bucket>>::iterator item = buckets_.find(key);
      if (item == buckets_.end()) {
        auto b = std::make_shared<bucket>(_config.capacity);
        bool res = b->consume_one(&_config, timestamp);
        buckets_[key] = b;
        return res;
      }
      return item->second->consume_one(&_config, timestamp);
    }

    //this can and will override bucket capacity but bucket will stay in correct state
    void _insert(std::shared_ptr<const krecord<K, V>> record, int64_t offset) override {
      current_offset_ = std::max<int64_t>(current_offset_, offset);
      if (record->value() == nullptr) {
        buckets_.erase(record->key());
      } else {
        typename std::map<K, std::shared_ptr<bucket>>::iterator item = buckets_.find(record->key());
        if (item == buckets_.end()) {
          auto b = std::make_shared<bucket>(_config.capacity);
          for (V i = 0; i != *record->value(); ++i)
            b->consume_one(&_config, record->event_time());
          buckets_[record->key()] = b;
          return;
        }
        for (V i = 0; i != *record->value(); ++i) // bug only works por posituve...
          item->second->consume_one(&_config, record->event_time());
      }
    }

    /**
    * Deletes a counter
    */
    void del(const K &key) {
      buckets_.erase(key);
    }

    /**
    * erases all counters
    */
    void clear() override {
      buckets_.clear();
      current_offset_ = -1;
    }

    /**
    * Returns the counter for the given key
    */
    std::shared_ptr<const kspp::krecord<K, V>> get(const K &key) const override {
      typename std::map<K, std::shared_ptr<bucket>>::const_iterator item = buckets_.find(key);
      if (item == buckets_.end()) {
        return std::make_shared<kspp::krecord<K, V>>(key, _config.capacity, -1);
      }
      return std::make_shared<kspp::krecord<K, V>>(key, item->second->token(), item->second->timestamp());
    }

    size_t aprox_size() const override {
      return buckets_.size();
    }

    size_t exact_size() const override {
      return buckets_.size();
    }

    typename kspp::materialized_source<K, V>::iterator begin(void) const override {
      return typename kspp::materialized_source<K, V>::iterator(
          std::make_shared<iterator_impl>(buckets_, iterator_impl::BEGIN));
    }

    typename kspp::materialized_source<K, V>::iterator end() const override {
      return typename kspp::materialized_source<K, V>::iterator(
          std::make_shared<iterator_impl>(buckets_, iterator_impl::END));
    }

  protected:
    const config _config;
    std::map<K, std::shared_ptr<bucket>> buckets_;
    int64_t current_offset_;
  };
}
