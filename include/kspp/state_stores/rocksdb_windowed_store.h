#include <memory>
#include <strstream>
#include <fstream>
#include <filesystem>
#include <glog/logging.h>

#include <kspp/kspp.h>
#include "state_store.h"

#ifdef WIN32
//you dont want to know why this is needed...
#undef max
#endif

#include <rocksdb/db.h>

#pragma once

namespace kspp {
  template<class K, class V, class CODEC>
  class rocksdb_windowed_store
      : public state_store<K, V> {
  public:
    enum {
      MAX_KEY_SIZE = 10000, MAX_VALUE_SIZE = 100000
    };

    class iterator_impl : public kmaterialized_source_iterator_impl<K, V> {
    public:
      enum seek_pos_e {
        BEGIN, END
      };

      iterator_impl(const std::map<int64_t, std::shared_ptr<rocksdb::DB>> &container, std::shared_ptr<CODEC> codec,
                    seek_pos_e pos)
          : _container(container), _outer_it(pos == BEGIN ? _container.begin() : _container.end()), _codec(codec) {
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

      bool valid() const override {
        return (_outer_it != _container.end());
      }

      void next() override {
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

      std::shared_ptr<const krecord<K, V>> item() const override {
        if (!_inner_it->Valid())
          return nullptr;
        rocksdb::Slice key = _inner_it->key();
        rocksdb::Slice value = _inner_it->value();

        int64_t timestamp = 0;
        // sanity - value size at least timestamp
        if (value.size() < sizeof(int64_t))
          return nullptr;
        memcpy(&timestamp, value.data(), sizeof(int64_t));
        K tmp_key;
        if (_codec->decode(key.data(), key.size(), tmp_key) != key.size())
          return nullptr;

        size_t actual_sz = value.size() - sizeof(int64_t); // remove timestamp
        auto tmp_value = std::make_shared<V>();
        size_t consumed = _codec->decode(value.data() + sizeof(int64_t), actual_sz, *tmp_value);
        if (consumed != actual_sz) {
          LOG(ERROR) << ", decode payload failed, consumed:" << consumed << ", actual sz:" << actual_sz;
          return nullptr;
        }
        return std::make_shared<krecord<K, V>>(tmp_key, tmp_value, timestamp);
      }

      bool operator==(const kmaterialized_source_iterator_impl<K, V> &other) const override {
        //fastpath...
        if (valid() && !other.valid())
          return false;
        if (!valid() && !other.valid())
          return true;
        if (valid() && other.valid())
          return (_outer_it->first == ((const iterator_impl &) other)._outer_it->first) &&
                 (_inner_it->key() == ((const iterator_impl &) other)._inner_it->key());
        return false;
      }

      inline rocksdb::Slice _key_slice() const {
        return _inner_it->key();
      }

    private:
      const std::map<int64_t, std::shared_ptr<rocksdb::DB>> &_container;
      typename std::map<int64_t, std::shared_ptr<rocksdb::DB>>::const_iterator _outer_it;
      typename std::unique_ptr<rocksdb::Iterator> _inner_it;
      std::shared_ptr<CODEC> _codec;
    };

    rocksdb_windowed_store(std::filesystem::path storage_path, std::chrono::milliseconds slot_width,
                           size_t nr_of_slots, std::shared_ptr<CODEC> codec = std::make_shared<CODEC>())
        : storage_path_(storage_path), offset_storage_path_(storage_path), slot_width_(slot_width.count()),
          nr_of_slots_(nr_of_slots), codec_(codec), current_offset_(kspp::OFFSET_BEGINNING),
          last_comitted_offset_(kspp::OFFSET_BEGINNING), last_flushed_offset_(kspp::OFFSET_BEGINNING),
          oldest_kept_slot_(-1) {
      LOG_IF(FATAL, storage_path.generic_string().size() == 0);
      std::filesystem::create_directories(storage_path);
      offset_storage_path_ /= "kspp_offset.bin";

      if (std::filesystem::exists(offset_storage_path_)) {
        std::ifstream is(offset_storage_path_.generic_string(), std::ios::binary);
        int64_t tmp;
        is.read((char *) &tmp, sizeof(int64_t));
        if (is.good()) {
          current_offset_ = tmp;
          last_comitted_offset_ = tmp;
          last_flushed_offset_ = tmp;
        }
      }
      // we must scan disk to load whats there...
      // we look for directories names with digits
    }

    ~rocksdb_windowed_store() override {
      close();
    }

    static std::string type_name() {
      return "rocksdb_windowed_store";
    }

    void close() override {
      buckets_.clear();
    }

    void garbage_collect(int64_t tick) override {
      oldest_kept_slot_ = get_slot_index(tick) - (nr_of_slots_ - 1);
      auto upper_bound = buckets_.lower_bound(oldest_kept_slot_);

      if (this->sink_) {
        std::vector<std::shared_ptr<krecord<K, V>>> tombstones;
        for (auto i = buckets_.begin(); i != upper_bound; ++i) {
          auto j = i->second->NewIterator(rocksdb::ReadOptions());
          j->SeekToFirst();
          while (j->Valid()) {
            rocksdb::Slice key = j->key();
            K tmp_key;
            if (codec_->decode(key.data(), key.size(), tmp_key) == key.size()) {
              auto record = std::make_shared<krecord<K, V>>(tmp_key, nullptr, tick);
              this->sink_(std::make_shared<kevent<K, V>>(record));
            }
            j->Next();
          }
        }
      }
      buckets_.erase(buckets_.begin(), upper_bound);
      // TBD get rid of database on disk...
    }

    // this respects strong ordering of timestamp and makes shure we only have one value
    // a bit slow but samantically correct
    // TBD add option to speed this up either by storing all values or disregarding timestamps
    void _insert(std::shared_ptr<const krecord<K, V>> record, int64_t offset) override {
      current_offset_ = std::max<int64_t>(current_offset_, offset);
      int64_t new_slot = get_slot_index(record->event_time());
      // old updates is killed straight away...
      if (new_slot < oldest_kept_slot_)
        return;

      char key_buf[MAX_KEY_SIZE];
      char val_buf[MAX_VALUE_SIZE];

      //_current_offset = std::max<int64_t>(_current_offset, record->offset());
      auto old_record = get(record->key());
      if (old_record && old_record->event_time() > record->event_time())
        return;

      std::shared_ptr<rocksdb::DB> bucket;
      {
        auto bucket_it = buckets_.find(new_slot);
        if (bucket_it == buckets_.end()) {
          rocksdb::Options options;
          options.IncreaseParallelism(); // should be #cores
          options.OptimizeLevelStyleCompaction();
          options.create_if_missing = true;
          std::filesystem::path path(storage_path_);
          path /= std::to_string(new_slot);
          rocksdb::DB *tmp = nullptr;
          auto s = rocksdb::DB::Open(options, path.generic_string(), &tmp);
          if (!s.ok()) {
            LOG(FATAL) << "rocksdb_windowed_store, failed to open rocks db, path:"
                       << path.generic_string();

            throw std::runtime_error(
                std::string("rocksdb_windowed_store, failed to open rocks db, path:") + path.generic_string());
          }
          auto it = buckets_.insert(
              std::pair<int64_t, std::shared_ptr<rocksdb::DB>>(new_slot, std::shared_ptr<rocksdb::DB>(tmp)));
          bucket = it.first->second;
        } else {
          bucket = bucket_it->second;
        }
      }

      //if we have an old value and it's from another slot - remove it.
      if (old_record) {
        int64_t old_slot = get_slot_index(old_record->event_time());
        if (old_slot != new_slot) {
          std::strstream s(key_buf, MAX_KEY_SIZE);
          size_t ksize = codec_->encode(record->key(), s);
          auto bucket_it = buckets_.find(old_slot);
          if (bucket_it != buckets_.end()) {
            auto status = bucket_it->second->Delete(rocksdb::WriteOptions(), rocksdb::Slice(key_buf, ksize));
            if (!status.ok()) {
              LOG(ERROR) << "rocksdb_windowed_store, delete failed, path:"
                         << storage_path_.generic_string() << ", slot:" << bucket_it->first;
            }
          }
        }
      }

      // write current data
      if (record->value()) {
        std::strstream ks(key_buf, MAX_KEY_SIZE);
        size_t ksize = codec_->encode(record->key(), ks);

        // write timestamp
        int64_t tmp = record->event_time();
        memcpy(val_buf, &tmp, sizeof(int64_t));
        std::strstream vs(val_buf + sizeof(int64_t), MAX_VALUE_SIZE - sizeof(int64_t));
        size_t vsize = codec_->encode(*record->value(), vs) + +sizeof(int64_t);

        rocksdb::Status status = bucket->Put(rocksdb::WriteOptions(), rocksdb::Slice((char *) key_buf, ksize),
                                             rocksdb::Slice(val_buf, vsize));
      } else {
        std::strstream s(key_buf, MAX_KEY_SIZE);
        size_t ksize = codec_->encode(record->key(), s);
        auto status = bucket->Delete(rocksdb::WriteOptions(), rocksdb::Slice(key_buf, ksize));
      }
    }

    std::shared_ptr<const krecord<K, V>> get(const K &key) const override {
      char key_buf[MAX_KEY_SIZE];
      size_t ksize = 0;
      {
        std::ostrstream s(key_buf, MAX_KEY_SIZE);
        ksize = codec_->encode(key, s);
      }

      for (auto &&i: buckets_) {
        std::string payload;
        rocksdb::Status s = i.second->Get(rocksdb::ReadOptions(), rocksdb::Slice(key_buf, ksize), &payload);
        if (s.ok()) {
          int64_t timestamp = 0;
          // sanity - at least timestamp
          if (payload.size() < sizeof(int64_t))
            return nullptr;
          memcpy(&timestamp, payload.data(), sizeof(int64_t));

          // read value
          size_t actual_sz = payload.size() - sizeof(int64_t);
          auto tmp_value = std::make_shared<V>();
          size_t consumed = codec_->decode(payload.data() + sizeof(int64_t), actual_sz, *tmp_value);
          if (consumed != actual_sz) {
            LOG(ERROR) << ", decode payload failed, consumed:" << consumed
                       << ", actual sz:" << actual_sz;
            return nullptr;
          }
          return std::make_shared<krecord<K, V>>(key, tmp_value, timestamp);
        }
      }
      return nullptr;
    }

    //should we allow writing -2 in store??
    void start(int64_t offset) override {
      current_offset_ = offset;
      commit(true);
    }

    /**
    * commits the offset
    */
    void commit(bool flush) override {
      last_comitted_offset_ = current_offset_;
      if (flush || ((last_comitted_offset_ - last_flushed_offset_) > 10000)) {
        if (last_flushed_offset_ != last_comitted_offset_) {
          std::ofstream os(offset_storage_path_.generic_string(), std::ios::binary);
          os.write((char *) &last_comitted_offset_, sizeof(int64_t));
          last_flushed_offset_ = last_comitted_offset_;
          os.flush();
        }
      }
    }

    /**
    * returns last offset
    */
    int64_t offset() const override {
      return current_offset_;
    }

    size_t aprox_size() const override {
      size_t count = 0;
      for (auto &i: buckets_) {
        std::string num;
        i.second->GetProperty("rocksdb.estimate-num-keys", &num);
        count += std::stoll(num);
      }
      return count;
    }

    size_t exact_size() const override {
      size_t sz = 0;
      for (const auto &i: *this)
        ++sz;
      return sz;
    }


    void clear() override {
      buckets_.clear(); // how do we kill database on disk?
      current_offset_ = kspp::OFFSET_BEGINNING;
    }

    typename kspp::materialized_source<K, V>::iterator begin(void) const override {
      return typename kspp::materialized_source<K, V>::iterator(
          std::make_shared<iterator_impl>(buckets_, codec_, iterator_impl::BEGIN));
    }

    typename kspp::materialized_source<K, V>::iterator end() const override {
      return typename kspp::materialized_source<K, V>::iterator(
          std::make_shared<iterator_impl>(buckets_, codec_, iterator_impl::END));
    }

  private:
    inline int64_t get_slot_index(int64_t timestamp) {
      return timestamp / slot_width_;
    }

    std::filesystem::path storage_path_;
    std::filesystem::path offset_storage_path_;
    std::map<int64_t, std::shared_ptr<rocksdb::DB>> buckets_;
    int64_t slot_width_;
    size_t nr_of_slots_;
    std::shared_ptr<CODEC> codec_;
    int64_t current_offset_;
    int64_t last_comitted_offset_;
    int64_t last_flushed_offset_;
    int64_t oldest_kept_slot_;
  };
}



