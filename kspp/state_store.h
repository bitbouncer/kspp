#pragma once
#include <memory>
#include <strstream>
#include <boost/filesystem.hpp>
#include <boost/log/trivial.hpp>
#include <rocksdb/db.h>
#include <kspp/kspp_defs.h>

namespace csi {
template<class K, class V, class CODEC>
class kstate_store
{
  public:
  enum { MAX_KEY_SIZE = 10000, MAX_VALUE_SIZE = 100000 };

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

    virtual bool operator==(const kmaterialized_source_iterator_impl<K, V>& other) const {
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

  kstate_store(std::string topic, int32_t partition, boost::filesystem::path storage_path, std::shared_ptr<CODEC> codec)
    : _codec(codec)
    , _topic(topic)
    , _partition(partition) {
    boost::filesystem::create_directories(boost::filesystem::path(storage_path));
    rocksdb::Options options;
    options.create_if_missing = true;
    rocksdb::DB* tmp = NULL;
    auto s = rocksdb::DB::Open(options, storage_path.generic_string(), &tmp);
    _db.reset(tmp);
    if (!s.ok()) {
      BOOST_LOG_TRIVIAL(error) << BOOST_CURRENT_FUNCTION << ", " << _topic << ":" << _partition << ", failed to open rocks db, path:" << storage_path.generic_string();
    }
    assert(s.ok());
  }

  ~kstate_store() {
    close();
  }
  void close() {
    _db = NULL;
    BOOST_LOG_TRIVIAL(info) << BOOST_CURRENT_FUNCTION << ", " << ", " << _topic << ":" << _partition;
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
      size_t consumed = _codec->decode(is, *res->value);
      if (consumed == 0) {
        BOOST_LOG_TRIVIAL(error) << BOOST_CURRENT_FUNCTION << ", " << _topic << ":" << _partition << ", decode payload failed, actual sz:" << payload.size();
        return NULL;
      } else if (consumed != payload.size()) {
        BOOST_LOG_TRIVIAL(error) << BOOST_CURRENT_FUNCTION << ", " << _topic << ":" << _partition << ", decode payload failed, consumed:" << consumed << ", actual sz:" << payload.size();
        return NULL;
      }
    }
    return res;
  }

  typename csi::kmaterialized_source<K, V>::iterator begin(void) {
    return typename csi::kmaterialized_source<K, V>::iterator(std::make_shared<iterator_impl>(_db.get(), _codec, iterator_impl::BEGIN));
  }

  typename csi::kmaterialized_source<K, V>::iterator end() {
    return typename csi::kmaterialized_source<K, V>::iterator(std::make_shared<iterator_impl>(_db.get(), _codec, iterator_impl::END));
  }

  private:
  std::string                           _topic;     // only used for logging to make sense...
  int32_t                               _partition; // only used for logging to make sense...
  std::unique_ptr<rocksdb::DB>          _db;        // maybee this should be a shared ptr since we're letting iterators out...
  std::shared_ptr<CODEC>                _codec;
};
}



