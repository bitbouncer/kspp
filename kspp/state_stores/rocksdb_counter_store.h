#pragma once
#include <memory>
#include <strstream>
#include <fstream>
#include <boost/filesystem.hpp>
#include <boost/log/trivial.hpp>
#include <rocksdb/db.h>
#include <rocksdb/merge_operator.h>
#include <kspp/kspp.h>
#include "state_store.h"

namespace kspp {
  class Int64AddOperator : public rocksdb::AssociativeMergeOperator {

  public:
    static bool Deserialize(const rocksdb::Slice& slice, int64_t* value) {
      if (slice.size() != sizeof(int64_t))
        return false;
      memcpy((void*)value, slice.data(), sizeof(int64_t));
      return true;
    }

    static void Serialize(int64_t val, std::string* dst) {
      dst->resize(sizeof(int64_t));
      memcpy((void*)dst->data(), &val, sizeof(int64_t));
    }

    static bool Deserialize(const std::string& src, int64_t* value) {
      if (src.size() != sizeof(int64_t))
        return false;
      memcpy((void*)value, src.data(), sizeof(int64_t));
      return true;
    }

  public:
    virtual bool Merge(
      const rocksdb::Slice& key,
      const rocksdb::Slice* existing_value,
      const rocksdb::Slice& value,
      std::string* new_value,
      rocksdb::Logger* logger) const override {

      // assuming 0 if no existing value
      int64_t existing = 0;
      if (existing_value) {
        if (!Deserialize(*existing_value, &existing)) {
          // if existing_value is corrupted, treat it as 0
          BOOST_LOG_TRIVIAL(error) << "Int64AddOperator::Merge existing_value value corruption";
          existing = 0;
        }
      }

      int64_t oper;
      if (!Deserialize(value, &oper)) {
        // if operand is corrupted, treat it as 0
        BOOST_LOG_TRIVIAL(error)  <<"Int64AddOperator::Merge, Deserialize operand value corruption";
        oper = 0;
      }

      Serialize(existing + oper, new_value);
      return true;        // always return true for this, since we treat all errors as "zero".
    }

    virtual const char* Name() const override {
      return "Int64AddOperator";
    }
  };

  template<class K, class V, class CODEC>
  class rocksdb_counter_store : public state_store<K, V>
  {
  public:
    enum { MAX_KEY_SIZE = 10000 };

    class iterator_impl : public kmaterialized_source_iterator_impl<K, V>
    {
    public:
      enum seek_pos_e { BEGIN, END };

      iterator_impl(rocksdb::DB* db, std::shared_ptr<CODEC> codec, seek_pos_e pos)
        : _it(db->NewIterator(rocksdb::ReadOptions()))
        , _codec(codec) {
        if (pos == BEGIN) {
          _it->SeekToFirst();
        } else {
          _it->SeekToLast(); // is there a better way to init to non valid??
          if (_it->Valid()) // if not valid the Next() calls fails...
            _it->Next(); // now it's invalid
        }
      }

      virtual bool valid() const {
        return _it->Valid();
      }

      virtual void next() {
        if (!_it->Valid())
          return;
        _it->Next();
      }

      virtual std::shared_ptr<const krecord<K, V>> item() const {
        if (!_it->Valid())
          return nullptr;
        rocksdb::Slice key = _it->key();
        rocksdb::Slice value = _it->value();

        K tmp_key;
        if (_codec->decode(key.data(), key.size(), tmp_key) != key.size())
          return nullptr;

        int64_t tmp_count = 0;
        if (!Int64AddOperator::Deserialize(value, &tmp_count))
          return nullptr;

        return std::make_shared<krecord<K, V>>(tmp_key, static_cast<V>(tmp_count), milliseconds_since_epoch()); // or should we use -1.
      }

      virtual bool operator==(const kmaterialized_source_iterator_impl<K, V>& other) const {
        //fastpath...
        if (valid() && !other.valid())
          return false;
        if (!valid() && !other.valid())
          return true;
        if (valid() && other.valid())
          return _it->key() == ((const iterator_impl&)other)._it->key();
        return false;
      }

      inline rocksdb::Slice _key_slice() const {
        return _it->key();
      }

    private:
      std::unique_ptr<rocksdb::Iterator> _it;
      std::shared_ptr<CODEC>             _codec;
    };

    rocksdb_counter_store(boost::filesystem::path storage_path, std::shared_ptr<CODEC> codec = std::make_shared<CODEC>())
      : _offset_storage_path(storage_path)
      , _codec(codec)
      , _current_offset(-1)
      , _last_comitted_offset(-1)
      , _last_flushed_offset(-1) {
      boost::filesystem::create_directories(boost::filesystem::path(storage_path));
      _offset_storage_path /= "kspp_offset.bin";
      rocksdb::Options options;
      options.merge_operator.reset(new Int64AddOperator);
      options.create_if_missing = true;
      rocksdb::DB* tmp = nullptr;
      auto s = rocksdb::DB::Open(options, storage_path.generic_string(), &tmp);
      _db.reset(tmp);
      if (!s.ok()) {
        BOOST_LOG_TRIVIAL(error) << " rocksdb_counter_store, failed to open rocks db, path:" << storage_path.generic_string();
      }

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
    }

    ~rocksdb_counter_store() {
      close();
    }

    void close() {
      _db = nullptr;
      //BOOST_LOG_TRIVIAL(info) << BOOST_CURRENT_FUNCTION << ", " << _name << " close()";
    }

    virtual void garbage_collect(int64_t tick) {
      // nothing to do
    }

    static std::string type_name() {
      return "rocksdb_counter_store";
    }

    /**
    * Put or delete a record
    */
    virtual void _insert(std::shared_ptr<const krecord<K, V>> record, int64_t offset) {
      _current_offset = std::max<int64_t>(_current_offset, offset);
      char key_buf[MAX_KEY_SIZE];
      size_t ksize = 0;
      std::strstream s(key_buf, MAX_KEY_SIZE);
      ksize = _codec->encode(record->key(), s);
      if (record->value())
      {
        std::string serialized;
        Int64AddOperator::Serialize((int64_t) *record->value(), &serialized);
        auto status = _db->Merge(rocksdb::WriteOptions(), rocksdb::Slice(key_buf, ksize), serialized);
      } else {
        auto status = _db->Delete(rocksdb::WriteOptions(), rocksdb::Slice(key_buf, ksize));
      }
    }

    std::shared_ptr<const krecord <K, V>> get(const K &key) const {
      char key_buf[MAX_KEY_SIZE];
      size_t ksize = 0;
      std::ostrstream os(key_buf, MAX_KEY_SIZE);
      ksize = _codec->encode(key, os);
      std::string str;
      auto status = _db->Get(rocksdb::ReadOptions(), rocksdb::Slice(key_buf, ksize), &str);
      int64_t count = 0;
      if (!Int64AddOperator::Deserialize(str, &count))
        return nullptr;

      auto res = std::make_shared<krecord<K, V>>(key, std::make_shared<V>((V) count), -1);
      return res;
    }

    /**
    * returns last offset
    */
    virtual int64_t offset() const {
      return _current_offset;
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

    virtual size_t aprox_size() const {
      std::string num;
      _db->GetProperty("rocksdb.estimate-num-keys", &num);
      return std::stoll(num);
    }

    virtual size_t exact_size() const {
      size_t sz = 0;
      for (const auto &i : *this)
        ++sz;
      return sz;
    }

    virtual void clear() {
      for (auto it = iterator_impl(_db.get(), _codec, iterator_impl::BEGIN), end_ = iterator_impl(_db.get(), _codec, iterator_impl::END); it!= end_; it.next()) {
        auto s = _db->Delete(rocksdb::WriteOptions(), it._key_slice());
      }
    }

    typename kspp::materialized_source<K, V>::iterator begin(void) const {
      return typename kspp::materialized_source<K, V>::iterator(std::make_shared<iterator_impl>(_db.get(), _codec, iterator_impl::BEGIN));
    }

    typename kspp::materialized_source<K, V>::iterator end() const {
      return typename kspp::materialized_source<K, V>::iterator(std::make_shared<iterator_impl>(_db.get(), _codec, iterator_impl::END));
    }

  private:
  boost::filesystem::path      _offset_storage_path;
  std::unique_ptr<rocksdb::DB> _db;    // maybe this should be a shared ptr since we're letting iterators out...
  std::shared_ptr<CODEC>       _codec;
  int64_t                      _current_offset;
  int64_t                      _last_comitted_offset;
  int64_t                      _last_flushed_offset;
  //const std::shared_ptr<rocksdb::ReadOptions>   _read_options;
  //const std::shared_ptr<rocksdb::WriteOptions>  _write_options;
  };
};
