#ifdef KSPP_ROCKSDB

#include <memory>
#include <strstream>
#include <fstream>
#include <filesystem>
#include <glog/logging.h>
#include <rocksdb/db.h>
#include <kspp/kspp.h>
#include "state_store.h"
#include <kspp/internal/rocksdb/rocksdb_operators.h>

#pragma once

namespace kspp {
  template<class K, class V, class CODEC>
  class rocksdb_counter_store : public state_store<K, V> {
  public:
    enum {
      MAX_KEY_SIZE = 10000
    };

    class iterator_impl : public kmaterialized_source_iterator_impl<K, V> {
    public:
      enum seek_pos_e {
        BEGIN, END
      };

      iterator_impl(rocksdb::DB *db, std::shared_ptr<CODEC> codec, seek_pos_e pos)
          : _it(db->NewIterator(rocksdb::ReadOptions())), _codec(codec) {
        if (pos == BEGIN) {
          _it->SeekToFirst();
        } else {
          _it->SeekToLast(); // is there a better way to init to non valid??
          if (_it->Valid()) // if not valid the Next() calls fails...
            _it->Next(); // now it's invalid
        }
      }

      bool valid() const override {
        return _it->Valid();
      }

      void next() override {
        if (!_it->Valid())
          return;
        _it->Next();
      }

      std::shared_ptr<const krecord<K, V>> item() const override {
        if (!_it->Valid())
          return nullptr;
        rocksdb::Slice key_slice = _it->key();
        rocksdb::Slice val_slice = _it->value();

        K key;
        if (_codec->decode(key_slice.data(), key_slice.size(), key) != key_slice.size())
          return nullptr;

        return std::make_shared<krecord<K, V>>(key, static_cast<V>(Int64AddOperator::Deserialize(val_slice)),
                                               milliseconds_since_epoch()); // or should we use -1.
      }

      bool operator==(const kmaterialized_source_iterator_impl<K, V> &other) const override {
        //fastpath...
        if (valid() && !other.valid())
          return false;
        if (!valid() && !other.valid())
          return true;
        if (valid() && other.valid())
          return _it->key() == ((const iterator_impl &) other)._it->key();
        return false;
      }

      inline rocksdb::Slice _key_slice() const {
        return _it->key();
      }

    private:
      std::unique_ptr<rocksdb::Iterator> _it;
      std::shared_ptr<CODEC> _codec;
    };

    rocksdb_counter_store(std::filesystem::path storage_path,
                          std::shared_ptr<CODEC> codec = std::make_shared<CODEC>())
        : offset_storage_path_(storage_path), codec_(codec), current_offset_(kspp::OFFSET_BEGINNING),
          last_comitted_offset_(kspp::OFFSET_BEGINNING), last_flushed_offset_(kspp::OFFSET_BEGINNING) {
      LOG_IF(FATAL, storage_path.generic_string().size() == 0);
      std::filesystem::create_directories(storage_path);
      offset_storage_path_ /= "kspp_offset.bin";
      rocksdb::Options options;
      options.IncreaseParallelism(); // should be #cores
      options.OptimizeLevelStyleCompaction();
      //options.merge_operator.reset(new Int64AddOperator);
      options.merge_operator = rocksdb::CreateInt64AddOperator();
      options.create_if_missing = true;
      rocksdb::DB *tmp = nullptr;
      auto s = rocksdb::DB::Open(options, storage_path.generic_string(), &tmp);
      db_.reset(tmp);
      if (!s.ok()) {
        LOG(FATAL) << "rocksdb_counter_store, failed to open rocks db, path:" << storage_path.generic_string();
        throw std::runtime_error(
            std::string("rocksdb_counter_store, failed to open rocks db, path:") + storage_path.generic_string());
      }

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
    }

    ~rocksdb_counter_store() override {
      close();
    }

    void close() override {
      db_ = nullptr;
      //BOOST_LOG_TRIVIAL(info) << BOOST_CURRENT_FUNCTION << ", " << _name << " close()";
    }

    void garbage_collect(int64_t tick) override {
      // nothing to do
    }

    static std::string type_name() {
      return "rocksdb_counter_store";
    }

    /**
    * Put or delete a record
    */
    void _insert(std::shared_ptr<const krecord<K, V>> record, int64_t offset) override {
      current_offset_ = std::max<int64_t>(current_offset_, offset);
      char key_buf[MAX_KEY_SIZE];
      size_t ksize = 0;
      std::strstream s(key_buf, MAX_KEY_SIZE);
      ksize = codec_->encode(record->key(), s);
      if (record->value()) {
        std::string serialized = Int64AddOperator::Serialize((int64_t) *record->value());
        auto status = db_->Merge(rocksdb::WriteOptions(), rocksdb::Slice(key_buf, ksize), serialized);
      } else {
        auto status = db_->Delete(rocksdb::WriteOptions(), rocksdb::Slice(key_buf, ksize));
      }
    }

    std::shared_ptr<const krecord<K, V>> get(const K &key) const override {
      char key_buf[MAX_KEY_SIZE];
      size_t ksize = 0;
      std::ostrstream os(key_buf, MAX_KEY_SIZE);
      ksize = codec_->encode(key, os);
      std::string str;
      auto status = db_->Get(rocksdb::ReadOptions(), rocksdb::Slice(key_buf, ksize), &str);
      if (!status.ok())
        return nullptr;
      auto res = std::make_shared<krecord<K, V>>(key, std::make_shared<V>((V) Int64AddOperator::Deserialize(str)), -1);
      return res;
    }

    /**
    * returns last offset
    */
    int64_t offset() const override {
      return current_offset_;
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

    size_t aprox_size() const override {
      std::string num;
      db_->GetProperty("rocksdb.estimate-num-keys", &num);
      return std::stoll(num);
    }

    size_t exact_size() const override {
      size_t sz = 0;
      for (const auto &i: *this)
        ++sz;
      return sz;
    }

    void clear() override {
      for (auto it = iterator_impl(db_.get(), codec_, iterator_impl::BEGIN), end_ = iterator_impl(db_.get(), codec_,
                                                                                                  iterator_impl::END);
           it != end_;
           it.next()) {
        auto s = db_->Delete(rocksdb::WriteOptions(), it._key_slice());
      }
      current_offset_ = kspp::OFFSET_BEGINNING;
    }

    typename kspp::materialized_source<K, V>::iterator begin(void) const override {
      return typename kspp::materialized_source<K, V>::iterator(
          std::make_shared<iterator_impl>(db_.get(), codec_, iterator_impl::BEGIN));
    }

    typename kspp::materialized_source<K, V>::iterator end() const override {
      return typename kspp::materialized_source<K, V>::iterator(
          std::make_shared<iterator_impl>(db_.get(), codec_, iterator_impl::END));
    }

  private:
    std::filesystem::path offset_storage_path_;
    std::unique_ptr<rocksdb::DB> db_;    // maybe this should be a shared ptr since we're letting iterators out...
    std::shared_ptr<CODEC> codec_;
    int64_t current_offset_;
    int64_t last_comitted_offset_;
    int64_t last_flushed_offset_;
  };
}
#endif