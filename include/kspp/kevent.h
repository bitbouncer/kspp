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
        : offset_(offset), cb_(callback) {
    }

    // two step init - used in commit chain
    //
    event_done_marker(std::function<void(int64_t offset, int32_t ec)> callback)
        : offset_(-1), cb_(callback) {
    }

    void init(int64_t offset) {
      offset_ = offset;
    }


    virtual ~event_done_marker() {
      if (cb_) // allow nullptr callback
        cb_(offset_, ec_);
    }

    inline int64_t offset() const {
      return offset_;
    }

    inline int32_t ec() const {
      return ec_;
    }

    inline void fail(int32_t ec) {
      if (ec)
        ec_ = ec;
    }


  protected:
    int64_t offset_;
    int32_t ec_=0;
    std::function<void(int64_t offset, int32_t ec)> cb_;
  };


  template<class K, class V>
  class kevent {
  public:
    kevent(std::shared_ptr<const krecord<K, V>> r, std::shared_ptr<event_done_marker> marker = nullptr)
        : record_(r), event_done_marker_(marker), partition_hash_(-1) {
    }

    kevent(std::shared_ptr<const krecord<K, V>> r, std::shared_ptr<event_done_marker> marker, uint32_t partition_hash)
        : record_(r), event_done_marker_(marker), partition_hash_(partition_hash) {
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