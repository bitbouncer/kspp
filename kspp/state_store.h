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
  enum { MAX_KEY_SIZE = 10000, MAX_VALUE_SIZE = 100000 };

  class kstate_store_iterator : public ktable_iterator_impl<K, V>
  {
    public:
    enum seek_pos_e { BEGIN, END };

    kstate_store_iterator(rocksdb::DB* db, std::shared_ptr<codec> codec, seek_pos_e pos)
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

    virtual std::shared_ptr<krecord<K, V>> item() const {
      if (!_it->Valid())
        return NULL;
      rocksdb::Slice key = _it->key();
      rocksdb::Slice value = _it->value();

      std::shared_ptr<krecord<K, V>> res(std::make_shared<krecord<K, V>>());
      res->offset = -1;
      res->event_time = -1; // ????
      res->value = std::make_shared<V>();

      std::istrstream isk(key.data(), key.size());
      if (_codec->decode(isk, res->key) == 0)
        return NULL;

      std::istrstream isv(value.data(), value.size());
      if (_codec->decode(isv, *res->value) == 0)
        return NULL;
      return res;
    }

    virtual bool operator==(const ktable_iterator_impl& other) const {
      //fastpath...
      if (valid() && !other.valid())
        return false;
      if (!valid() && !other.valid())
        return true;
      if (valid() && other.valid())
        return _it->key() == ((const kstate_store_iterator&) other)._it->key();
      return false;
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
    rocksdb::Status s = _db->Put(rocksdb::WriteOptions(), rocksdb::Slice((char*) key_buf, ksize), rocksdb::Slice(val_buf, vsize));
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

  std::shared_ptr<krecord<K, V>> get(const K& key) {
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
    auto  res = std::make_shared<krecord<K, V>>();
    res->key = key;
    res->offset = -1;
    res->event_time = -1; // ????
    {
      std::istrstream is(payload.data(), payload.size());
      res->value = std::make_shared<V>();
      size_t sz = _codec->decode(is, *res->value);
      if (sz == 0)
        return NULL;
    }
    return res;
  }

  typename csi::ktable<K, V>::iterator begin(void) {
    return csi::ktable<K, V>::iterator(std::make_shared<kstate_store_iterator>(_db.get(), _codec, kstate_store_iterator::BEGIN));
  }

  typename csi::ktable<K, V>::iterator end() {
    return csi::ktable<K, V>::iterator(std::make_shared<kstate_store_iterator>(_db.get(), _codec, kstate_store_iterator::END));
  }

  private:
  std::unique_ptr<rocksdb::DB>          _db; // maybee this should be a shared ptr since we're letting iterators out...
  std::shared_ptr<codec>                _codec;
};
}



