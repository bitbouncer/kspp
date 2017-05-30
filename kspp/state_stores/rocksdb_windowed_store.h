#pragma once
#include <memory>
#include <strstream>
#include <fstream>
#include <boost/filesystem.hpp>
#include <boost/log/trivial.hpp>

#include <kspp/kspp.h>
#include "state_store.h"

#ifdef WIN32
//you dont want to know why this is needed...
#undef max
#endif
#include <rocksdb/db.h>

namespace kspp {
template<class K, class V, class CODEC>
class rocksdb_windowed_store
  : public state_store<K, V>
{
  public:
  enum { MAX_KEY_SIZE = 10000, MAX_VALUE_SIZE = 100000 };

  class iterator_impl : public kmaterialized_source_iterator_impl<K, V>
  {
    public:
    enum seek_pos_e { BEGIN, END };

    iterator_impl(std::map<int64_t, std::shared_ptr<rocksdb::DB>>& container, std::shared_ptr<CODEC> codec, seek_pos_e pos)
      : _container(container)
      , _outer_it(pos == BEGIN ? _container.begin() : _container.end())
      , _codec(codec) {
      if (pos == BEGIN) {
        // skip empty buckets
        while (_outer_it != _container.end()) {
          _inner_it.reset(_outer_it->second->NewIterator(rocksdb::ReadOptions()));
          _inner_it->SeekToFirst();
          if (_inner_it->Valid())
            break;
          ++_outer_it;
        }
        
        if (_outer_it == _container.end())
          return;
      }
    }

    virtual bool valid() const {
      return (_outer_it != _container.end());
    }

    virtual void next() {
      if (_outer_it == _container.end())
        return;

      _inner_it->Next();
      if (!_inner_it->Valid())
        ++_outer_it;

      // skip empty buckets
      while (_outer_it != _container.end()) {
        _inner_it.reset(_outer_it->second->NewIterator(rocksdb::ReadOptions()));
        _inner_it->SeekToFirst();
        if (_inner_it->Valid())
          break;
        ++_outer_it;
      }
    }

    virtual std::shared_ptr<const krecord<K, V>> item() const {
      if (!_inner_it->Valid())
        return nullptr;
      rocksdb::Slice key = _inner_it->key();
      rocksdb::Slice value = _inner_it->value();

      int64_t timestamp = 0;
      // sanity - value size at least timestamp
      if (value.size() < sizeof(int64_t))
        return nullptr;
      memcpy(&timestamp, value.data(), sizeof(int64_t));
      auto  res = std::make_shared<krecord<K, V>>(K(), std::make_shared<V>(), timestamp);

      if (_codec->decode(key.data(), key.size(), res->key) != key.size())
        return nullptr;

      size_t actual_sz = value.size() - sizeof(int64_t); // remove timestamp
      size_t consumed = _codec->decode(value.data() + sizeof(int64_t), actual_sz, *res->value);
      if (consumed != actual_sz) {
        BOOST_LOG_TRIVIAL(error) << BOOST_CURRENT_FUNCTION << ", decode payload failed, consumed:" << consumed << ", actual sz:" << actual_sz;
        return nullptr;
      }
      return res;
    }

    virtual bool operator==(const kmaterialized_source_iterator_impl<K, V>& other) const {
      //fastpath...
      if (valid() && !other.valid())
        return false;
      if (!valid() && !other.valid())
        return true;
      if (valid() && other.valid())
        return (_outer_it->first == ((const iterator_impl&) other)._outer_it->first) && (_inner_it->key() == ((const iterator_impl&) other)._inner_it->key());
      return false;
    }

    inline rocksdb::Slice _key_slice() const {
      return _inner_it->key();
    }

    private:
    std::map<int64_t, std::shared_ptr<rocksdb::DB>>&                    _container;
    typename std::map<int64_t, std::shared_ptr<rocksdb::DB>>::iterator  _outer_it;
    typename std::unique_ptr<rocksdb::Iterator>                         _inner_it;
    std::shared_ptr<CODEC>                                              _codec;
  };

  rocksdb_windowed_store(boost::filesystem::path storage_path, std::chrono::milliseconds slot_width, size_t nr_of_slots, std::shared_ptr<CODEC> codec = std::make_shared<CODEC>())
    : _storage_path(storage_path)
    , _offset_storage_path(storage_path)
    , _slot_width(slot_width.count())
    , _nr_of_slots(nr_of_slots)
    , _codec(codec)
    , _current_offset(-1)
    , _last_comitted_offset(-1)
    , _last_flushed_offset(-1)
    , _oldest_kept_slot(-1) {
    boost::filesystem::create_directories(boost::filesystem::path(storage_path));
    _offset_storage_path /= "kspp_offset.bin";

    if (boost::filesystem::exists(_offset_storage_path)) {
      std::ifstream is(_offset_storage_path.generic_string(), std::ios::binary);
      int64_t tmp;
      is.read((char*) &tmp, sizeof(int64_t));
      if (is.good()) {
        _current_offset = tmp;
        _last_comitted_offset = tmp;
        _last_flushed_offset = tmp;
      }
    }
    // we must scan disk to load whats there...
    // we look for directories names with digits
  }

  ~rocksdb_windowed_store() {
    close();
  }

  static std::string type_name() {
    return "rocksdb_windowed_store";
  }

  void close() {
    _buckets.clear();
  }

  virtual void garbage_collect(int64_t tick) {
    _oldest_kept_slot = get_slot_index(tick) - (_nr_of_slots-1);
    auto upper_bound = _buckets.lower_bound(_oldest_kept_slot);

    if (this->_sink) {
      std::vector<std::shared_ptr<krecord<K, V>>> tombstones;
      for (auto i = _buckets.begin(); i != upper_bound; ++i) {
        auto j = i->second->NewIterator(rocksdb::ReadOptions());
        j->SeekToFirst();
        while (j->Valid()) {
          auto record = std::make_shared<krecord<K, V>>(K(), nullptr, tick);
          rocksdb::Slice key = j->key();
          if (_codec->decode(key.data(), key.size(), record->key) == key.size())
            this->_sink(std::make_shared<kevent<K,V>>(record));
          j->Next();
        }
      }
    }
    _buckets.erase(_buckets.begin(), upper_bound);
    // TBD get rid of database on disk...
  }

  // this respects strong ordering of timestamp and makes shure we only have one value
  // a bit slow but samantically correct
  // TBD add option to speed this up either by storing all values or disregarding timestamps
  virtual void _insert(std::shared_ptr<const krecord<K, V>> record, int64_t offset) {
    _current_offset = std::max<int64_t>(_current_offset, offset);
    int64_t new_slot = get_slot_index(record->event_time);
    // old updates is killed straight away...
    if (new_slot < _oldest_kept_slot)
      return;
    
    char key_buf[MAX_KEY_SIZE];
    char val_buf[MAX_VALUE_SIZE];

    //_current_offset = std::max<int64_t>(_current_offset, record->offset());
    auto old_record = get(record->key);
    if (old_record && old_record->event_time > record->event_time)
      return;

    std::shared_ptr<rocksdb::DB> bucket;
    {
      auto bucket_it = _buckets.find(new_slot);
      if (bucket_it == _buckets.end()) {
        rocksdb::Options options;
        options.create_if_missing = true;
        boost::filesystem::path path(_storage_path);
        path /= std::to_string(new_slot);
        rocksdb::DB* tmp = nullptr;
        auto s = rocksdb::DB::Open(options, path.generic_string(), &tmp);
        if (!s.ok()) {
          BOOST_LOG_TRIVIAL(error) << BOOST_CURRENT_FUNCTION << ", failed to open rocks db, path:" << path.generic_string();
          return;
        }
        auto it = _buckets.insert(std::pair<int64_t, std::shared_ptr<rocksdb::DB>>(new_slot, std::shared_ptr<rocksdb::DB>(tmp)));
        bucket = it.first->second;
      } else {
        bucket = bucket_it->second;
      }
    }

    //if we have an old value and it's from another slot - remove it.
    if (old_record) {
      int64_t old_slot = get_slot_index(old_record->event_time);
      if (old_slot != new_slot) {
        std::strstream s(key_buf, MAX_KEY_SIZE);
        size_t ksize = _codec->encode(record->key, s);
        auto bucket_it = _buckets.find(old_slot);
        if (bucket_it != _buckets.end()) {
          auto status = bucket_it->second->Delete(rocksdb::WriteOptions(), rocksdb::Slice(key_buf, ksize));
          if (!status.ok()) {
            BOOST_LOG_TRIVIAL(error) << BOOST_CURRENT_FUNCTION << ", Delete failed, path:" << _storage_path.generic_string() << ", slot:" << bucket_it->first;
          }
        }
      }
    }

    // write current data
    if (record->value) {
      std::strstream ks(key_buf, MAX_KEY_SIZE);
      size_t ksize = _codec->encode(record->key, ks);

      // write timestamp
      memcpy(val_buf, &record->event_time, sizeof(int64_t));
      std::strstream vs(val_buf + sizeof(int64_t), MAX_VALUE_SIZE - sizeof(int64_t));
      size_t vsize = _codec->encode(*record->value, vs) + +sizeof(int64_t);

      rocksdb::Status status = bucket->Put(rocksdb::WriteOptions(), rocksdb::Slice((char*) key_buf, ksize), rocksdb::Slice(val_buf, vsize));
    } else {
      std::strstream s(key_buf, MAX_KEY_SIZE);
      size_t ksize = _codec->encode(record->key, s);
      auto status = bucket->Delete(rocksdb::WriteOptions(), rocksdb::Slice(key_buf, ksize));
    }
  }

  std::shared_ptr<const krecord<K, V>> get(const K& key) {
    char key_buf[MAX_KEY_SIZE];
    size_t ksize = 0;
    {
      std::ostrstream s(key_buf, MAX_KEY_SIZE);
      ksize = _codec->encode(key, s);
    }

    for (auto&& i : _buckets) {
      std::string payload;
      rocksdb::Status s = i.second->Get(rocksdb::ReadOptions(), rocksdb::Slice(key_buf, ksize), &payload);
      if (s.ok()) {
        int64_t timestamp = 0;
        // sanity - at least timestamp
        if (payload.size() < sizeof(int64_t))
          return nullptr;
        memcpy(&timestamp, payload.data(), sizeof(int64_t));
        auto  res = std::make_shared<krecord<K, V>>(key, std::make_shared<V>(), timestamp);
        // read value
        size_t actual_sz = payload.size() - sizeof(int64_t);
        size_t consumed = _codec->decode(payload.data() + sizeof(int64_t), actual_sz, *res->value);
        if (consumed != actual_sz) {
          BOOST_LOG_TRIVIAL(error) << BOOST_CURRENT_FUNCTION << ", decode payload failed, consumed:" << consumed << ", actual sz:" << actual_sz;
          return nullptr;
        }
        return res;
      }
    }
    return nullptr;
  }

  //should we allow writing -2 in store??
  virtual void start(int64_t offset) {
    _current_offset = offset;
    commit(true);
  }

  /**
  * commits the offset
  */
  virtual void commit(bool flush) {
    _last_comitted_offset = _current_offset;
    if (flush || ((_last_comitted_offset - _last_flushed_offset) > 10000)) {
      if (_last_flushed_offset != _last_comitted_offset) {
        std::ofstream os(_offset_storage_path.generic_string(), std::ios::binary);
        os.write((char*) &_last_comitted_offset, sizeof(int64_t));
        _last_flushed_offset = _last_comitted_offset;
        os.flush();
      }
    }
  }

  /**
  * returns last offset
  */
  virtual int64_t offset() const {
    return _current_offset;
  }

  virtual size_t size() const {
    size_t count = 0;
    for (auto& i : _buckets) {
      std::string num;
      i.second->GetProperty("rocksdb.estimate-num-keys", &num);
      count += std::stoll(num);
    }
    return count;
  }

  virtual void clear() {
    _buckets.clear(); // how do we kill database on disk?
    _current_offset = -1;
  }

  typename kspp::materialized_source<K, V>::iterator begin(void) {
    return typename kspp::materialized_source<K, V>::iterator(std::make_shared<iterator_impl>(_buckets, _codec, iterator_impl::BEGIN));
  }

  typename kspp::materialized_source<K, V>::iterator end() {
    return typename kspp::materialized_source<K, V>::iterator(std::make_shared<iterator_impl>(_buckets, _codec, iterator_impl::END));
  }

  private:
  inline int64_t get_slot_index(int64_t timestamp) {
    return timestamp / _slot_width;
  }

  boost::filesystem::path                         _storage_path;
  boost::filesystem::path                         _offset_storage_path;
  std::map<int64_t, std::shared_ptr<rocksdb::DB>> _buckets;
  int64_t                                         _slot_width;
  size_t                                          _nr_of_slots;
  std::shared_ptr<CODEC>                          _codec;
  int64_t                                         _current_offset;
  int64_t                                         _last_comitted_offset;
  int64_t                                         _last_flushed_offset;
  int64_t                                         _oldest_kept_slot;
};
}



