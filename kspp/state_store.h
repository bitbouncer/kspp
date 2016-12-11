#pragma once
#include <memory>
#include <strstream>
#include <boost/filesystem.hpp>
#include <rocksdb/db.h>
#include <kspp/kspp_defs.h>

namespace csi {
  template<class K, class V, class codec>
  class kstate_store
  {
  public:
  enum { MAX_KEY_SIZE = 10000, MAX_VALUE_SIZE= 100000 };
  
  class kstate_store_iterator : public ktable_iterator<K, V>
  {
  public:
    kstate_store_iterator(rocksdb::DB* db, std::shared_ptr<codec> codec)
      : _it(db->NewIterator(rocksdb::ReadOptions()))
      , _codec(codec) {
      _it->SeekToFirst();
    }

    virtual bool valid() const {
      return _it->Valid();
    }

    virtual void next() {
      if (!_it->Valid())
        return;
      _it->Next();
    }

    virtual std::unique_ptr<krecord<K, V>> item() const {
      if (!_it->Valid())
        return NULL;
      rocksdb::Slice key   = _it->key();
      rocksdb::Slice value = _it->value();

      std::unique_ptr<krecord<K, V>> res(new krecord<K, V>());
      res->offset = -1;
      res->event_time = -1; // ????
      res->value = std::unique_ptr<V>(new V());

      std::istrstream isk(key.data(), key.size());
      if (_codec->decode(isk, res->key) == 0)
        return NULL;

      std::istrstream isv(value.data(), value.size());
      if (_codec->decode(isv, *res->value)==0)
        return NULL;
      return res;
    }

  private:
    std::unique_ptr<rocksdb::Iterator> _it;
    std::shared_ptr<codec>             _codec;
  };

    kstate_store(boost::filesystem::path storage_path, std::shared_ptr<codec> codec) :
      _codec(codec) {
      boost::filesystem::create_directories(boost::filesystem::path(storage_path));
      rocksdb::Options options;
      options.create_if_missing = true;
      rocksdb::DB* tmp = NULL;
      auto s = rocksdb::DB::Open(options, storage_path.generic_string(), &tmp);
      _db.reset(tmp);
      if (!s.ok()) {
        std::cerr << "failed to open rocks db, path:" << storage_path.generic_string() << std::endl;
      }
      assert(s.ok());
    }

    ~kstate_store() {
      close();
    }
    void close() {
      _db = NULL;
    }

    void put(const K& key, const V& val) {
      char key_buf[MAX_KEY_SIZE];
      char val_buf[MAX_VALUE_SIZE];

      size_t ksize = 0;
      size_t vsize = 0;
      {
        std::strstream s(key_buf, MAX_KEY_SIZE);
        ksize = _codec->encode(key, s);
      }
      {
        std::strstream s(val_buf, MAX_VALUE_SIZE);
        vsize = _codec->encode(val, s);
      }
      rocksdb::Status s = _db->Put(rocksdb::WriteOptions(), rocksdb::Slice((char*)key_buf, ksize), rocksdb::Slice(val_buf, vsize));
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

    std::unique_ptr<krecord<K, V>> get(const K& key) {
      char key_buf[MAX_KEY_SIZE];
      size_t ksize = 0;
      {
        std::ostrstream s(key_buf, MAX_KEY_SIZE);
        ksize = _codec->encode(key, s);
      }

      std::string payload;
      rocksdb::Status s = _db->Get(rocksdb::ReadOptions(), rocksdb::Slice(key_buf, ksize), &payload);
      if (!s.ok())
        return NULL;
      std::unique_ptr<krecord<K, V>> res(new krecord<K, V>());
      res->key = key;
      res->offset = -1;
      res->event_time = -1; // ????
      {
        std::istrstream is(payload.data(), payload.size());
        res->value = std::unique_ptr<V>(new V());
        size_t sz = _codec->decode(is, *res->value);
        if (sz == 0)
          return NULL;
      }
      return res;
    }

    std::shared_ptr<csi::ktable_iterator<K,V>> iterator() {
      return std::make_shared<kstate_store_iterator>(_db.get(), _codec);
    }

  private:
    std::unique_ptr<rocksdb::DB> _db; // maybee this should be a shared ptr since we're letting iterators out...
    std::shared_ptr<codec>       _codec;
  };
}



