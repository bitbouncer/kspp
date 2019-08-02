#include <cstdint>
#include <cassert>
#include <memory>
#include <functional>
#include <kspp/krecord.h>
#pragma once

namespace kspp {
  class event_done_marker {
  public:
    event_done_marker(std::function<void(int64_t offset, int32_t ec)> callback)
        : _offset(-1)
        , _ec(0)
        , _cb(callback) {
    }

    virtual ~event_done_marker(){
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

    void init(int64_t offset) {
      _offset = offset;
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
            : _record(r)
              , _event_done_marker(marker)
              , _partition_hash(-1) {
    }

    kevent(std::shared_ptr<const krecord<K, V>> r, std::shared_ptr<event_done_marker> marker, uint32_t partition_hash)
            : _record(r)
              , _event_done_marker(marker)
              , _partition_hash(partition_hash) {
    }

    inline int64_t event_time() const {
      return _record ? _record->event_time() : -1;
    }

    inline int64_t offset() const {
      return _event_done_marker ? _event_done_marker->offset() : -1;
    }

    inline std::shared_ptr<const krecord<K, V>> record() const {
      return _record;
    }

    inline std::shared_ptr<event_done_marker> id() {
      return _event_done_marker;
    }

    inline bool has_partition_hash() const {
      return _partition_hash >= 0;
    }

    inline uint32_t partition_hash() const {
      assert(_partition_hash >= 0);
      return static_cast<uint32_t>(_partition_hash);
    }

  private:
    std::shared_ptr<const krecord<K, V>> _record;
    std::shared_ptr<event_done_marker> _event_done_marker;
    const int64_t _partition_hash;
  };

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

}