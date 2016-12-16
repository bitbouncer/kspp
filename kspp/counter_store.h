#pragma once
#include <memory>
#include <strstream>
#include <boost/filesystem.hpp>
#include <boost/log/trivial.hpp>
#include <rocksdb/db.h>
#include <rocksdb/merge_operator.h>
#include <kspp/kspp_defs.h>
//
//namespace csi {
//template<class K, class codec>
//class kkeycounter_store
//{
//  public:
//  enum { MAX_KEY_SIZE = 10000 };
//
//  class iterator : public ktable_iterator_impl<K, size_t>
//  {
//    public:
//    enum seek_pos_e { BEGIN, END };
//
//    iterator(rocksdb::DB* db, std::shared_ptr<codec> codec, seek_pos_e pos)
//      : _it(db->NewIterator(rocksdb::ReadOptions()))
//      , _codec(codec) {
//      if (pos == BEGIN) {
//        _it->SeekToFirst();
//      } else {
//        _it->SeekToLast(); // is there a better way to init to non valid??
//        if (_it->Valid()) // if not valid the Next() calls fails...
//          _it->Next(); // now it's invalid
//      }
//    }
//
//    virtual bool valid() const {
//      return _it->Valid();
//    }
//
//    virtual void next() {
//      if (!_it->Valid())
//        return;
//      _it->Next();
//    }
//
//    virtual std::shared_ptr<krecord<K, size_t>> item() const {
//      if (!_it->Valid())
//        return NULL;
//      rocksdb::Slice key = _it->key();
//      rocksdb::Slice value = _it->value();
//
//      std::shared_ptr<krecord<K, size_t>> res(std::make_shared<krecord<K, size_t>>());
//      res->offset = -1;
//      res->event_time = -1; // ????
//      res->value = std::make_shared<size_t>();
//      *res->value = 1; // BUG...
//
//      std::istrstream isk(key.data(), key.size());
//      if (_codec->decode(isk, res->key) == 0)
//        return NULL;
//
//      //std::istrstream isv(value.data(), value.size());
//      //if (_codec->decode(isv, *res->value) == 0)
//      //  return NULL;
//      return res;
//    }
//
//    virtual bool operator==(const ktable_iterator_impl& other) const {
//      //fastpath...
//      if (valid() && !other.valid())
//        return false;
//      if (!valid() && !other.valid())
//        return true;
//      if (valid() && other.valid())
//        return _it->key() == ((const kstate_store_iterator&) other)._it->key();
//      return false;
//    }
//
//    private:
//    std::unique_ptr<rocksdb::Iterator> _it;
//    std::shared_ptr<codec>             _codec;
//
//  };
//
//  kkeycounter_store(std::string name, boost::filesystem::path storage_path, std::shared_ptr<codec> codec)
//    : _codec(codec)
//    , _name("keycounter_store-" + name) {
//    boost::filesystem::create_directories(boost::filesystem::path(storage_path));
//    rocksdb::Options options;
//    options.create_if_missing = true;
//    rocksdb::DB* tmp = NULL;
//    auto s = rocksdb::DB::Open(options, storage_path.generic_string(), &tmp);
//    _db.reset(tmp);
//    if (!s.ok()) {
//      BOOST_LOG_TRIVIAL(error) << BOOST_CURRENT_FUNCTION << ", " << _topic << ":" << _partition << ", failed to open rocks db, path:" << storage_path.generic_string();
//    }
//    assert(s.ok());
//  }
//
//  ~kkeycounter_store() {
//    close();
//  }
//  void close() {
//    _db = NULL;
//    BOOST_LOG_TRIVIAL(info) << BOOST_CURRENT_FUNCTION << ", " << ", " << _topic << ":" << _partition;
//  }
//
//  size_t put(const K& key) {
//    char key_buf[MAX_KEY_SIZE];
//    size_t ksize = 0;
//    std::strstream s(key_buf, MAX_KEY_SIZE);
//    ksize = _codec->encode(key, s);
//    rocksdb::Status s = _db->Put(rocksdb::WriteOptions(), rocksdb::Slice(key_buf, ksize), rocksdb::Slice(NULL, 0));
//    return 1; // BUG should be number of items with "key" value
//  }
//
//  /*void put(std::shared_ptr<krecord<K, V>> r) {
//    char key_buf[MAX_KEY_SIZE];
//    char val_buf[MAX_VALUE_SIZE];
//
//    size_t ksize = 0;
//    size_t vsize = 0;
//    {
//      std::strstream s(key_buf, MAX_KEY_SIZE);
//      ksize = _codec->encode(r->key, s);
//    }
//
//    if (r->value)
//    {
//      std::strstream s(val_buf, MAX_VALUE_SIZE);
//      vsize = _codec->encode(*r->value, s);
//    }
//    rocksdb::Status s = _db->Put(rocksdb::WriteOptions(), rocksdb::Slice((char*) key_buf, ksize), rocksdb::Slice(val_buf, vsize));
//  }*/
//
//  void del(const K& key) {
//    char key_buf[MAX_KEY_SIZE];
//    size_t ksize = 0;
//    {
//      std::strstream s(key_buf, MAX_KEY_SIZE);
//      ksize = _codec->encode(key, s);
//    }
//    auto s = _db->Delete(rocksdb::WriteOptions(), rocksdb::Slice(key_buf, ksize));
//  }
//
//  //std::shared_ptr<krecord<K, V>> get(const K& key) {
//  //  char key_buf[MAX_KEY_SIZE];
//  //  size_t ksize = 0;
//  //  {
//  //    std::ostrstream s(key_buf, MAX_KEY_SIZE);
//  //    ksize = _codec->encode(key, s);
//  //  }
//
//  //  std::string payload;
//  //  rocksdb::Status s = _db->Get(rocksdb::ReadOptions(), rocksdb::Slice(key_buf, ksize), &payload);
//  //  if (!s.ok())
//  //    return NULL;
//  //  auto  res = std::make_shared<krecord<K, V>>();
//  //  res->key = key;
//  //  res->offset = -1;
//  //  res->event_time = -1; // ????
//  //  {
//  //    std::istrstream is(payload.data(), payload.size());
//  //    res->value = std::make_shared<V>();
//  //    size_t consumed = _codec->decode(is, *res->value);
//  //    if (consumed == 0) {
//  //      BOOST_LOG_TRIVIAL(error) << BOOST_CURRENT_FUNCTION << ", " << _topic << ":" << _partition << ", decode payload failed, actual sz:" << payload.size();
//  //      return NULL;
//  //    } else if (consumed != payload.size()) {
//  //      BOOST_LOG_TRIVIAL(error) << BOOST_CURRENT_FUNCTION << ", " << _topic << ":" << _partition << ", decode payload failed, consumed:" << consumed << ", actual sz:" << payload.size();
//  //      return NULL;
//  //    }
//  //  }
//  //  return res;
//  //}
//
//  typename csi::ktable<K, size_t>::iterator begin(void) {
//    return csi::ktable<K, size_t>::iterator(std::make_shared<kstate_store_iterator>(_db.get(), _codec, kstate_store_iterator::BEGIN));
//  }
//
//  typename csi::ktable<K, size_t>::iterator end() {
//    return csi::ktable<K, size_t>::iterator(std::make_shared<kstate_store_iterator>(_db.get(), _codec, kstate_store_iterator::END));
//  }
//
//  private:
//  std::string                           _name;     // only used for logging to make sense...
//  std::unique_ptr<rocksdb::DB>          _db;        // maybee this should be a shared ptr since we're letting iterators out...
//  std::shared_ptr<codec>                _codec;
//};


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
  class kkeycounter_store
  {
  public:
      enum { MAX_KEY_SIZE = 10000 };

      kkeycounter_store(std::string name, boost::filesystem::path storage_path, std::shared_ptr<CODEC> codec)
        : _codec(codec)
        , _name("keycounter_store-" + name) {
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
    
      ~kkeycounter_store() {
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
    
      /*virtual bool Get(const string& key, uint64_t *value) {
        string str;
        auto s = db_->Get(get_option_, key, &str);
        if (s.ok()) {
          *value = Deserialize(str);
          return true;
        } else {
          return false;
        }
      }*/

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
    
     /* typename csi::ktable<K, size_t>::iterator begin(void) {
        return csi::ktable<K, size_t>::iterator(std::make_shared<kstate_store_iterator>(_db.get(), _codec, kstate_store_iterator::BEGIN));
      }
    
      typename csi::ktable<K, size_t>::iterator end() {
        return csi::ktable<K, size_t>::iterator(std::make_shared<kstate_store_iterator>(_db.get(), _codec, kstate_store_iterator::END));
      }*/
    
      private:
      std::string                                   _name;     // only used for logging to make sense...
      std::unique_ptr<rocksdb::DB>                  _db;        // maybee this should be a shared ptr since we're letting iterators out...
      //const std::shared_ptr<rocksdb::ReadOptions>   _read_options;
      //const std::shared_ptr<rocksdb::WriteOptions>  _write_options;
      std::shared_ptr<CODEC>                        _codec;
    };
};
