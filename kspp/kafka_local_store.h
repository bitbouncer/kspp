#pragma once
#include <memory>
#include <strstream>
#include <boost/filesystem.hpp>
#include <rocksdb/db.h>
#include <librdkafka/rdkafkacpp.h>
#include "encoder.h"
#include "kspp_defs.h"

namespace csi {

  /*
  class kafka_local_store
  {
    public:
    kafka_local_store(boost::filesystem::path storage_path);
    ~kafka_local_store();
    void close();
    void put(RdKafka::Message*);
    std::unique_ptr<RdKafka::Message> get(const void* key, size_t key_size);
    private:
    rocksdb::DB* _db;
  };
  */

  template<class K, class V, class codec>
  class kstate_store
  {
  public:
    kstate_store(boost::filesystem::path storage_path, std::shared_ptr<codec> codec) :
      _db(NULL),
      _codec(codec) {
      boost::filesystem::create_directories(boost::filesystem::path(storage_path));
      rocksdb::Options options;
      options.create_if_missing = true;
      auto s = rocksdb::DB::Open(options, storage_path.generic_string(), &_db);
      assert(s.ok());
    }

    ~kstate_store() {
      close();
    }
    void close() {
      delete _db;
      _db = NULL;
    }

    void put(const K& key, const V& val) {
      char key_buf[1000];
      char val_buf[50000];

      size_t ksize = 0;
      size_t vsize = 0;
      {
        std::strstream s(key_buf, 1000);
        ksize = _codec->encode(key, s);
      }
      {
        std::strstream s(val_buf, 50000);
        vsize = _codec->encode(val, s);
      }
      rocksdb::Status s = _db->Put(rocksdb::WriteOptions(), rocksdb::Slice((char*)key_buf, ksize), rocksdb::Slice(val_buf, vsize));
    }

    void del(const K& key) {
      char key_buf[1000];
      size_t ksize = 0;
      {
        std::strstream s(key_buf, 1000);
        ksize = _codec->encode(key, s);
      }
      auto s = _db->Delete(rocksdb::WriteOptions(), rocksdb::Slice(key_buf, ksize));
    }


    std::unique_ptr<krecord<K, V>> get(const K& key) {
      char key_buf[1000];
      size_t ksize = 0;
      {
        std::ostrstream s(key_buf, 1000);
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
    rocksdb::DB*           _db;
    std::shared_ptr<codec> _codec;
  };
}



