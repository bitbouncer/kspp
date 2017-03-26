#include "state_store.h"
#include <map>
#include <chrono>

namespace kspp {
  template<class K, class V, class CODEC = void>
  class mem_windowed_store
    : public state_store<K, V> {
    
    typedef std::map<K, std::shared_ptr<krecord<K, V>>> bucket_type;

  public:
    class iterator_impl
      : public kmaterialized_source_iterator_impl<K, V> {
      

    public:
      enum seek_pos_e { BEGIN, END };

      iterator_impl(std::map <int64_t, std::shared_ptr<bucket_type>>& container, seek_pos_e pos)
        : _container(container)
        , _outer_it(pos == BEGIN ? _container.begin() : _container.end()) {
        if (pos == BEGIN) {
          // skip empty buckets
          while (_outer_it != _container.end() && _outer_it->second->size() == 0)
            ++_outer_it;

          if (_outer_it == _container.end())
            return;

          _inner_it = _outer_it->second->begin();
        }
      }

      virtual bool valid() const {
        return  _outer_it != _container.end();
      }

      virtual void next() {
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

      virtual std::shared_ptr<krecord<K, V>> item() const {
        return (_outer_it == _container.end()) ? nullptr : _inner_it->second;
      }

      virtual bool operator==(const kmaterialized_source_iterator_impl<K, V>& other) const {
        if (valid() && !other.valid())
          return false;
        if (!valid() && !other.valid())
          return true;
        if (valid() && other.valid())
          return (_outer_it->first == ((const iterator_impl&)other)._outer_it->first) && (_inner_it->first == ((const iterator_impl&)other)._inner_it->first);
        return false;
      }

    private:
      std::map <int64_t, std::shared_ptr<bucket_type>>&                                                   _container;
      typename std::map <int64_t, std::shared_ptr<std::map<K, std::shared_ptr<krecord<K, V>>>>>::iterator _outer_it;
      typename std::map<K, std::shared_ptr<krecord<K, V>>>::iterator                                      _inner_it;
    };

    mem_windowed_store(boost::filesystem::path storage_path, std::chrono::milliseconds slot_width, size_t nr_of_slots)
      : _slot_width(slot_width.count())
      , _nr_of_slots(nr_of_slots) {
    }

    virtual ~mem_windowed_store() {
    }

    static std::string type_name() {
      return "mem_windowed_store";
    }

    virtual void close() {
    }

    virtual void garbage_collect(int64_t tick) {
      _oldest_kept_slot = get_slot_index(tick) - (_nr_of_slots - 1);
      auto upper_bound = _buckets.lower_bound(_oldest_kept_slot);

      if (this->_sink) {
        std::vector<std::shared_ptr<krecord<K, V>>> tombstones;
        for (auto i = _buckets.begin(); i != upper_bound; ++i) {
          for (auto&& j : *i->second)
            this->_sink(std::make_shared<ktransaction<K,V>>(std::make_shared<krecord<K, V>>(j.first)));
        }
      }
      _buckets.erase(_buckets.begin(), upper_bound);
    }

    /**
    * Put a key-value pair
    */
    virtual void _insert(std::shared_ptr<ktransaction<K, V>> transaction) {
      _current_offset = std::max<int64_t>(_current_offset, transaction->offset());
      auto record = transaction->record();

      int64_t new_slot = get_slot_index(record->event_time);
      // old updates is killed straight away...
      if (new_slot < _oldest_kept_slot)
        return;

      auto old_record = get(record->key);
      if (old_record == nullptr) {
        if (record->value) {
          auto bucket_it = _buckets.find(new_slot);
          if (bucket_it == _buckets.end()) { // new slot  
            auto it = _buckets.insert(std::pair<int64_t, std::shared_ptr<bucket_type>>(new_slot, std::make_shared<bucket_type>()));
            std::shared_ptr<bucket_type> bucket = it.first->second;
            (*bucket)[record->key] = record;
          } else { // existing slot 
            (*bucket_it->second)[record->key] = record;
          }
        }
        return;
      }

      // skip if we have a newer value
      if (old_record->event_time > record->event_time)
        return;

      int64_t old_slot = get_slot_index(old_record->event_time);

      if (record->value==nullptr) {
        auto bucket_it = _buckets.find(old_slot);
        assert(bucket_it != _buckets.end()); // should never fail - we know we have an old value
        if (bucket_it != _buckets.end()) 
          bucket_it->second->erase(record->key);
        return;
      }

      if (new_slot == old_slot) { // same slot
        auto bucket_it = _buckets.find(old_slot);
        assert(bucket_it != _buckets.end()); // should never fail - we know we have an old value
        if (bucket_it != _buckets.end())
          (*bucket_it->second)[record->key] = record;
      } else { // not same slot 
        // kill old value
        auto bucket_it = _buckets.find(old_slot);
        assert(bucket_it != _buckets.end()); // should never fail - we know we have an old value
        if (bucket_it != _buckets.end())
          (*bucket_it->second).erase(record->key);
        // insert new value
        bucket_it = _buckets.find(new_slot);
        if (bucket_it == _buckets.end()) {  // new slot  
          auto it = _buckets.insert(std::pair<int64_t, std::shared_ptr<bucket_type>>(new_slot, std::make_shared<bucket_type>()));
          std::shared_ptr<bucket_type> bucket = it.first->second;
          (*bucket)[record->key] = record;
        } else { // existing slot 
          (*bucket_it->second)[record->key] = record;
        }
      }
    }

    /**
    * commits the offset
    */
    virtual void commit(bool flush) {
      // noop
    }

    /**
    * returns last offset
    */
    virtual int64_t offset() const {
      return _current_offset;
    }

    virtual void start(int64_t offset) {
      _current_offset = offset;
    }

    /**
    * Returns a key-value pair with the given key
    */
    virtual std::shared_ptr<krecord<K, V>> get(const K& key) {
      for (auto&& i : _buckets) {
        auto item = i.second->find(key);
        if (item != i.second->end())
          return item->second;
      }
      return nullptr; // tbd
      //auto it = _store.find(key);
      //return (it == _store.end()) ? nullptr : it->second;
    }

    virtual size_t size() const {
      size_t sz = 0;
      for (auto&& i : _buckets)
        sz += i.second->size();
      return sz;
    }

    virtual void clear() {
      _buckets.clear();
      _current_offset = -1;
    }

    typename kspp::materialized_source<K, V>::iterator begin(void) {
      return typename kspp::materialized_source<K, V>::iterator(std::make_shared<iterator_impl>(_buckets, iterator_impl::BEGIN));
    }

    typename kspp::materialized_source<K, V>::iterator end() {
      return typename kspp::materialized_source<K, V>::iterator(std::make_shared<iterator_impl>(_buckets, iterator_impl::END));
    }

  private:
    inline int64_t get_slot_index(int64_t timestamp) {
      return timestamp / _slot_width;
    }

    //virtual std::shared_ptr<ktransaction<K, V>> get_from_slot(const K& key, int64_t slot_begin, int64_t slot_end) {
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


    std::shared_ptr<kspp::partition_source<K, V>>   _source;
    std::map<int64_t, std::shared_ptr<bucket_type>> _buckets;
    int64_t                                         _slot_width;
    size_t                                          _nr_of_slots;
    int64_t                                         _oldest_kept_slot;
    int64_t                                         _current_offset;
  };
}