#include "state_store.h"
#include <map>
#include <chrono>

#pragma once

namespace kspp {
  template<class K, class V, class CODEC = void>
  class mem_windowed_store
      : public state_store<K, V> {

    typedef std::map<K, std::shared_ptr<const krecord<K, V>>> bucket_type;

  public:
    class iterator_impl
        : public kmaterialized_source_iterator_impl<K, V> {

    public:
      enum seek_pos_e {
        BEGIN, END
      };

      iterator_impl(const std::map<int64_t, std::shared_ptr<bucket_type>> &container, seek_pos_e pos)
          : _container(container), _outer_it(pos == BEGIN ? _container.begin() : _container.end()) {
        if (pos == BEGIN) {
          // skip empty buckets
          while (_outer_it != _container.end() && _outer_it->second->size() == 0)
            ++_outer_it;

          if (_outer_it == _container.end())
            return;

          _inner_it = _outer_it->second->begin();
        }
      }

      bool valid() const override {
        return _outer_it != _container.end();
      }

      void next() override {
        if (_outer_it == _container.end())
          return;
        ++_inner_it;
        if (_inner_it == _outer_it->second->end()) {
          ++_outer_it;
          // skip over empty buckets
          while (_outer_it != _container.end() && _outer_it->second->size() == 0)
            ++_outer_it;
          if (_outer_it == _container.end())
            return;
          _inner_it = _outer_it->second->begin();
        }
      }

      std::shared_ptr<const krecord<K, V>> item() const override {
        return (_outer_it == _container.end()) ? nullptr : _inner_it->second;
      }

      bool operator==(const kmaterialized_source_iterator_impl<K, V> &other) const override {
        if (valid() && !other.valid())
          return false;
        if (!valid() && !other.valid())
          return true;
        if (valid() && other.valid())
          return (_outer_it->first == ((const iterator_impl &) other)._outer_it->first) &&
                 (_inner_it->first == ((const iterator_impl &) other)._inner_it->first);
        return false;
      }

    private:
      const std::map<int64_t, std::shared_ptr<bucket_type>> &_container;
      typename std::map<int64_t, std::shared_ptr<std::map<K, std::shared_ptr<const krecord<K, V>>>>>::const_iterator _outer_it;
      typename std::map<K, std::shared_ptr<const krecord<K, V>>>::const_iterator _inner_it;
    };

    mem_windowed_store(std::experimental::filesystem::path storage_path, std::chrono::milliseconds slot_width,
                       size_t nr_of_slots)
        : slot_width_(slot_width.count()), nr_of_slots_(nr_of_slots), oldest_kept_slot_(0) {
    }

    static std::string type_name() {
      return "mem_windowed_store";
    }

    void close() override {
    }

    void garbage_collect(int64_t tick) override {
      oldest_kept_slot_ = get_slot_index(tick) - (nr_of_slots_ - 1);
      auto upper_bound = buckets_.lower_bound(oldest_kept_slot_);

      if (this->sink_) {
        //std::vector<std::shared_ptr<krecord<K, V>>> tombstones;
        for (auto &&i = buckets_.begin(); i != upper_bound; ++i) {
          for (auto &&j: *i->second)
            this->sink_(std::make_shared<kevent<K, V>>(std::make_shared<krecord<K, V>>(j.first, nullptr, tick)));
        }
      }
      buckets_.erase(buckets_.begin(), upper_bound);
    }

    /**
     * deletes oldest record
     *
     * @param tick
     */
    void garbage_collect_one(int64_t tick) override {
      for (auto &&i: buckets_) {
        if (i.second->size() > 0) {
          K oldest_key;
          int64_t oldest_ts = INT64_MAX;
          for (auto &&item: *i.second) {
            if (item.second->event_time() <= oldest_ts) {
              oldest_ts = item.second->event_time();
              oldest_key = item.second->key();
            }
          }
          i.second->erase(oldest_key);
          if (this->sink_)
            this->sink_(std::make_shared<kevent<K, V>>(std::make_shared<krecord<K, V>>(oldest_key, nullptr, tick)));
          return; // do this once and return
        }
      }
    }

    /**
    * Put a key-value pair
    */
    void _insert(std::shared_ptr<const krecord<K, V>> record, int64_t offset) override {
      current_offset_ = std::max<int64_t>(current_offset_, offset);
      int64_t new_slot = get_slot_index(record->event_time());
      // old updates is killed straight away...
      if (new_slot < oldest_kept_slot_)
        return;

      auto old_record = get(record->key());
      if (old_record == nullptr) {
        if (record->value()) {
          auto bucket_it = buckets_.find(new_slot);
          if (bucket_it == buckets_.end()) { // new slot
            auto it = buckets_.insert(
                std::pair<int64_t, std::shared_ptr<bucket_type>>(new_slot, std::make_shared<bucket_type>()));
            std::shared_ptr<bucket_type> bucket = it.first->second;
            (*bucket)[record->key()] = record;
          } else { // existing slot 
            (*bucket_it->second)[record->key()] = record;
          }
        }
        return;
      }

      // skip if we have a newer value
      if (old_record->event_time() > record->event_time())
        return;

      int64_t old_slot = get_slot_index(old_record->event_time());

      if (record->value() == nullptr) {
        auto bucket_it = buckets_.find(old_slot);
        assert(bucket_it != buckets_.end()); // should never fail - we know we have an old value
        if (bucket_it != buckets_.end())
          bucket_it->second->erase(record->key());
        return;
      }

      if (new_slot == old_slot) { // same slot
        auto bucket_it = buckets_.find(old_slot);
        assert(bucket_it != buckets_.end()); // should never fail - we know we have an old value
        if (bucket_it != buckets_.end())
          (*bucket_it->second)[record->key()] = record;
      } else { // not same slot 
        // kill old value
        auto bucket_it = buckets_.find(old_slot);
        assert(bucket_it != buckets_.end()); // should never fail - we know we have an old value
        if (bucket_it != buckets_.end())
          (*bucket_it->second).erase(record->key());
        // insert new value
        bucket_it = buckets_.find(new_slot);
        if (bucket_it == buckets_.end()) {  // new slot
          auto it = buckets_.insert(
              std::pair<int64_t, std::shared_ptr<bucket_type>>(new_slot, std::make_shared<bucket_type>()));
          std::shared_ptr<bucket_type> bucket = it.first->second;
          (*bucket)[record->key()] = record;
          assert(get_slot_index(record->event_time()) == new_slot); // make sure this item is in right slot...
          assert(it.first->first == new_slot); // make sure this item is in right slot...
        } else { // existing slot 
          (*bucket_it->second)[record->key()] = record;
          assert(get_slot_index(record->event_time()) == new_slot); // make sure this item is in right slot...
          assert(bucket_it->first == new_slot); // make sure this item is in right slot...
        }
      }
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
      for (auto &&i: buckets_) {
        auto item = i.second->find(key);
        if (item != i.second->end()) {
          //int64_t expected = get_slot_index(item->second->event_time());
          assert(get_slot_index(item->second->event_time()) == i.first); // make sure this item is in right slot...
          return item->second;
        }
      }
      return nullptr; // tbd
      //auto it = _store.find(key);
      //return (it == _store.end()) ? nullptr : it->second;
    }

    size_t aprox_size() const override {
      size_t sz = 0;
      for (auto &&i: buckets_)
        sz += i.second->size();
      return sz;
    }

    size_t exact_size() const override {
      size_t sz = 0;
      for (auto &&i: buckets_)
        sz += i.second->size();
      return sz;
    }

    void clear() override {
      buckets_.clear();
      current_offset_ = -1;
    }

    typename kspp::materialized_source<K, V>::iterator begin(void) const override {
      return typename kspp::materialized_source<K, V>::iterator(
          std::make_shared<iterator_impl>(buckets_, iterator_impl::BEGIN));
    }

    typename kspp::materialized_source<K, V>::iterator end() const override {
      return typename kspp::materialized_source<K, V>::iterator(
          std::make_shared<iterator_impl>(buckets_, iterator_impl::END));
    }

  private:
    inline int64_t get_slot_index(int64_t timestamp) const {
      return timestamp / slot_width_;
    }

    //virtual std::shared_ptr<kevent<K, V>> get_from_slot(const K& key, int64_t slot_begin, int64_t slot_end) {
    //  for (auto&& i : _buckets) {
    //    if (i.first >= slot_begin && i.first < slot_end) {
    //      auto item = i.second->find(key);
    //      if (item != i.second->end())
    //        return item->second;
    //    }
    //  }
    //  return nullptr; // tbd
    //                  //auto it = _store.find(key);
    //                  //return (it == _store.end()) ? nullptr : it->second;
    //}


    //std::shared_ptr<kspp::partition_source<K, V>> source_;
    std::map<int64_t, std::shared_ptr<bucket_type>> buckets_;
    int64_t slot_width_;
    size_t nr_of_slots_;
    int64_t oldest_kept_slot_;
    int64_t current_offset_;
  };
}
