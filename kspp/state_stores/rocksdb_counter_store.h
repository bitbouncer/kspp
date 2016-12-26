#pragma once
#include <memory>
#include <strstream>
#include <boost/filesystem.hpp>
#include <boost/log/trivial.hpp>
#include <rocksdb/db.h>
#include <rocksdb/merge_operator.h>
#include <kspp/kspp_defs.h>
#include "counter_store.h"

namespace csi {
  class UInt64AddOperator : public rocksdb::AssociativeMergeOperator {

  public:
    static bool Deserialize(const rocksdb::Slice& slice, uint64_t* value) {
      if (slice.size() != sizeof(int64_t))
        return false;
      memcpy((void*) value, slice.data(), sizeof(int64_t));
      return true;
    }

    static void Serialize(uint64_t val, std::string* dst) {
      dst->resize(sizeof(uint64_t));
      memcpy((void*)dst->data(), &val, sizeof(uint64_t));
    }

    static bool Deserialize(const std::string& src, uint64_t* value) {
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
      uint64_t existing = 0;
      if (existing_value) {
        if (!Deserialize(*existing_value, &existing)) {
          // if existing_value is corrupted, treat it as 0
          Log(logger, "existing value corruption");
          existing = 0;
        }
      }

      uint64_t oper;
      if (!Deserialize(value, &oper)) {
        // if operand is corrupted, treat it as 0
        Log(logger, "operand value corruption");
        oper = 0;
      }

      Serialize(existing + oper, new_value);
      return true;        // always return true for this, since we treat all errors as "zero".
    }

    virtual const char* Name() const override {
      return "UInt64AddOperator";
    }
  };

  template<class K, class CODEC>
  class rocksdb_counter_store : public counter_store<K>
  {
  public:
      enum { MAX_KEY_SIZE = 10000 };

      class iterator_impl : public kmaterialized_source_iterator_impl<K, size_t>
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

        virtual std::shared_ptr<krecord<K, size_t>> item() const {
          if (!_it->Valid())
            return NULL;
          rocksdb::Slice key = _it->key();
          rocksdb::Slice value = _it->value();

          std::shared_ptr<krecord<K, size_t>> res(std::make_shared<krecord<K, size_t>>());
          res->offset = -1;
          res->event_time = -1; // ????
          res->value = std::make_shared<size_t>();

          std::istrstream isk(key.data(), key.size());
          if (_codec->decode(isk, res->key) == 0)
            return NULL;

          uint64_t count = 0;
          if (!UInt64AddOperator::Deserialize(value, &count))             {
            return NULL;
          }
          *res->value = count;
          return res;
        }

        virtual bool operator==(const kmaterialized_source_iterator_impl<K, size_t>& other) const {
          //fastpath...
          if (valid() && !other.valid())
            return false;
          if (!valid() && !other.valid())
            return true;
          if (valid() && other.valid())
            return _it->key() == ((const iterator_impl&) other)._it->key();
          return false;
        }

        private:
        std::unique_ptr<rocksdb::Iterator> _it;
        std::shared_ptr<CODEC>             _codec;

      };

      rocksdb_counter_store(std::string name, boost::filesystem::path storage_path, std::shared_ptr<CODEC> codec)
        : _codec(codec)
        , _name(name+ "-(rocksdb_counter_store)") {
        boost::filesystem::create_directories(boost::filesystem::path(storage_path));
        rocksdb::Options options;
        options.merge_operator.reset(new UInt64AddOperator);
        options.create_if_missing = true;
        rocksdb::DB* tmp = NULL;
        auto s = rocksdb::DB::Open(options, storage_path.generic_string(), &tmp);
        _db.reset(tmp);
        if (!s.ok()) {
          BOOST_LOG_TRIVIAL(error) << BOOST_CURRENT_FUNCTION << ", " << _name << ", failed to open rocks db, path:" << storage_path.generic_string();
        }
        assert(s.ok());
      }
    
      ~rocksdb_counter_store() {
        close();
      }

      void close() {
        _db = NULL;
        BOOST_LOG_TRIVIAL(info) << BOOST_CURRENT_FUNCTION << ", " << _name << " close()";
      }
    
      void add(const K& key, size_t val) {
        char key_buf[MAX_KEY_SIZE];
        size_t ksize = 0;
        std::strstream s(key_buf, MAX_KEY_SIZE);
        ksize = _codec->encode(key, s);
        std::string serialized;
        UInt64AddOperator::Serialize(val, &serialized);
        auto status = _db->Merge(rocksdb::WriteOptions(), rocksdb::Slice(key_buf, ksize), serialized);
      }
    
      void del(const K& key) {
        char key_buf[MAX_KEY_SIZE];
        size_t ksize = 0;
        {
          std::strstream s(key_buf, MAX_KEY_SIZE);
          ksize = _codec->encode(key, s);
        }
        auto s = _db->Delete(rocksdb::WriteOptions(), rocksdb::Slice(key_buf, ksize));
      }

      size_t get(const K& key) {
        char key_buf[MAX_KEY_SIZE];
        size_t ksize = 0;
        std::ostrstream os(key_buf, MAX_KEY_SIZE);
        ksize = _codec->encode(key, os);
        std::string str;
        auto status = _db->Get(rocksdb::ReadOptions(), rocksdb::Slice(key_buf, ksize), &str);
        uint64_t count = 0;
        bool res = UInt64AddOperator::Deserialize(str, &count);
        return count;
      }

      typename csi::materialized_partition_source<K, size_t>::iterator begin(void) {
        return typename csi::materialized_partition_source<K, size_t>::iterator(std::make_shared<iterator_impl>(_db.get(), _codec, iterator_impl::BEGIN));
      }

      typename csi::materialized_partition_source<K, size_t>::iterator end() {
        return typename csi::materialized_partition_source<K, size_t>::iterator(std::make_shared<iterator_impl>(_db.get(), _codec, iterator_impl::END));
      }
    
      private:
      std::string                                   _name;     // only used for logging to make sense...
      std::unique_ptr<rocksdb::DB>                  _db;        // maybee this should be a shared ptr since we're letting iterators out...
      //const std::shared_ptr<rocksdb::ReadOptions>   _read_options;
      //const std::shared_ptr<rocksdb::WriteOptions>  _write_options;
      std::shared_ptr<CODEC>                        _codec;
    };
};
