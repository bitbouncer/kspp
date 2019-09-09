#include <cstdint>
#include <cassert>
#include <memory>
#include <functional>
#include <kspp/krecord.h>
#pragma once

namespace kspp {
  class event_done_marker {
  public:
    // init when you know the offset right away
    event_done_marker(int64_t offset, std::function<void(int64_t offset, int32_t ec)> callback)
        : _offset(offset)
        , _ec(0)
        , _cb(callback) {
    }

    // two step init - used in commit chain
    //
    event_done_marker(std::function<void(int64_t offset, int32_t ec)> callback)
        : _offset(-1)
        , _ec(0)
        , _cb(callback) {
    }

    void init(int64_t offset) {
      _offset = offset;
    }


    virtual ~event_done_marker(){
      if (_cb) // allow nullptr callback
        _cb(_offset, _ec);
    }

    inline int64_t offset() const {
      return _offset;
    }

    inline int32_t ec() const {
      return _ec;
    }

    inline void fail(int32_t ec) {
      if (ec)
        _ec = ec;
    }


  protected:
    int64_t _offset;
    int32_t _ec;
    std::function<void(int64_t offset, int32_t ec)> _cb;
  };


  template<class K, class V>
  class kevent {
  public:
    kevent(std::shared_ptr<const krecord<K, V>> r, std::shared_ptr<event_done_marker> marker = nullptr)
        : record_(r)
        , event_done_marker_(marker)
        , partition_hash_(-1) {
    }

    kevent(std::shared_ptr<const krecord<K, V>> r, std::shared_ptr<event_done_marker> marker, uint32_t partition_hash)
        : record_(r)
        , event_done_marker_(marker)
        , partition_hash_(partition_hash) {
    }

    inline int64_t event_time() const {
      return record_ ? record_->event_time() : -1;
    }

    inline int64_t offset() const {
      return event_done_marker_ ? event_done_marker_->offset() : -1;
    }

    inline std::shared_ptr<const krecord<K, V>> record() const {
      return record_;
    }

    inline std::shared_ptr<event_done_marker> id() {
      return event_done_marker_;
    }

    inline bool has_partition_hash() const {
      return partition_hash_ >= 0;
    }

    inline uint32_t partition_hash() const {
      assert(partition_hash_ >= 0);
      return static_cast<uint32_t>(partition_hash_);
    }

  private:
    std::shared_ptr<const krecord<K, V>> record_;
    std::shared_ptr<event_done_marker> event_done_marker_;
    const int64_t partition_hash_;
  };

  /*
  template<class K, class V>
  std::shared_ptr<kevent<K, V>> make_event(const K &key, const V &value, int64_t ts = kspp::milliseconds_since_epoch(), std::shared_ptr<event_done_marker> marker = nullptr){
    auto record = std::make_shared<krecord<K, V>>(key, value, ts);
    return std::make_shared<kevent<K, V>>(record, marker);
  }

  template<class K>
  std::shared_ptr<kevent<K, void>> make_event(const K &key, int64_t ts = kspp::milliseconds_since_epoch(), std::shared_ptr<event_done_marker> marker = nullptr){
    auto record = std::make_shared<krecord<K, void>>(key, ts);
    return std::make_shared<kevent<K, void>>(record, marker);
  }

  template<class V>
    std::shared_ptr<kevent<void, V>> make_event(const V &value, int64_t ts = kspp::milliseconds_since_epoch(), std::shared_ptr<event_done_marker> marker = nullptr){
    auto record = std::make_shared<krecord<void, V>>(value, ts);
    return std::make_shared<kevent<void, V>>(record, marker);
  }
  */
}