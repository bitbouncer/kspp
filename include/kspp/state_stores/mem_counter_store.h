#include "state_store.h"
#include <map>

#pragma once

namespace kspp {
  template<class K, class V, class CODEC=void>
  class mem_counter_store
      : public state_store<K, V> {
  public:
    class iterator_impl
        : public kmaterialized_source_iterator_impl<K, V> {
    public:
      enum seek_pos_e {
        BEGIN, END
      };

      iterator_impl(const std::map<K, std::shared_ptr<const krecord<K, V>>> &container, seek_pos_e pos)
          : _container(container), _it(pos == BEGIN ? _container.begin() : _container.end()) {
      }

      bool valid() const override {
        return _it != _container.end();
      }

      void next() override {
        if (_it == _container.end())
          return;
        ++_it;
      }

      std::shared_ptr<const krecord<K, V>> item() const override {
        return (_it == _container.end()) ? nullptr : _it->second;
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
      const std::map<K, std::shared_ptr<const krecord<K, V>>> &_container;
      typename std::map<K, std::shared_ptr<const krecord<K, V>>>::const_iterator _it;
    };

    mem_counter_store(std::experimental::filesystem::path storage_path) {
    }

    static std::string type_name() {
      return "mem_counter_store";
    }

    void close() override {}

    /**
    * Put a key-value pair
    */
    void _insert(std::shared_ptr<const krecord<K, V>> record, int64_t offset) override {
      current_offset_ = std::max<int64_t>(current_offset_, offset);
      auto item = store_.find(record->key());

      // non existing - create - TBD should we keep a tombstone???
      if (item == store_.end()) {
        if (record->value())
          store_[record->key()] = record;
        return;
      }

      // we accept aggregation on old timestamps 
      // note that we need to create a new stored record for each update since we would update potentially live message otherwise
      if (record->value()) {
        V new_value = *(item->second->value()) + *(record->value());
        int64_t timestamp = std::max<int64_t>(item->second->event_time(), record->event_time());
        item->second = std::make_shared<krecord<K, V>>(item->first, new_value, timestamp);
        return;
      }

      // do not delete if we have a newer value
      if (item->second->event_time() > record->event_time())
        return;

      store_.erase(record->key());
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
    * Returns a key-value pair with the given key
    */
    std::shared_ptr<const krecord<K, V>> get(const K &key) const override {
      auto it = store_.find(key);
      return (it == store_.end()) ? nullptr : it->second;
    }

    void clear() override {
      store_.clear();
      current_offset_ = -1;
    }

    size_t aprox_size() const override {
      return store_.size();
    }

    size_t exact_size() const override {
      return store_.size();
    }


    typename kspp::materialized_source<K, V>::iterator begin(void) const override {
      return typename kspp::materialized_source<K, V>::iterator(
          std::make_shared<iterator_impl>(store_, iterator_impl::BEGIN));
    }

    typename kspp::materialized_source<K, V>::iterator end() const override {
      return typename kspp::materialized_source<K, V>::iterator(
          std::make_shared<iterator_impl>(store_, iterator_impl::END));
    }

  private:
    std::map<K, std::shared_ptr<const krecord<K, V>>> store_;
    int64_t current_offset_;
  };
}
