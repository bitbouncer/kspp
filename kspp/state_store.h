#pragma once
#include <memory>
#include <strstream>
#include <boost/filesystem.hpp>
#include <rocksdb/db.h>

namespace csi {
  template<class K, class V, class codec>
  class kstate_store
  {
  public:
  enum { MAX_KEY_SIZE = 10000, MAX_VALUE_SIZE= 100000 };

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
  private:
    std::unique_ptr<rocksdb::DB> _db;
    std::shared_ptr<codec>       _codec;
  };
}



