#include <functional>
#include <memory>
#include <ctype.h>
#include <string>
#include <kspp/utils/spinlock.h>
#include <kspp/kevent.h>
#pragma once

namespace kspp {

  void autocommit_marker_gc();

  class commit_chain {
  public:

    class autocommit_marker : public event_done_marker {
    public:
      autocommit_marker(std::function<void(int64_t offset, int32_t ec)> callback)
              : event_done_marker(callback){
      }

      ~autocommit_marker() override;

      void init(int64_t offset, std::shared_ptr<autocommit_marker> next) {
        event_done_marker::init(offset);
        //_offset = offset;
        _next = next;
      }

    private:
      std::shared_ptr<autocommit_marker> _next;
    };

  public:
    commit_chain(std::string topic, int32_t partition);

    std::shared_ptr<commit_chain::autocommit_marker> create(int64_t offset);

    // nr of outstanding requests
    inline size_t size() const {
      return _size;
    }

    inline int64_t last_good_offset() const {
      return _last_good_offset;
    }

    // first error code
    inline int32_t first_ec() const {
      return _first_ec;
    }

  private:
    void handle_result(int64_t offset, int32_t ec);

    const std::string _topic;
    const int32_t _partition;
    mutable spinlock _spinlock;
    volatile size_t _size;
    int64_t _last_good_offset;
    int32_t _first_ec;
    std::shared_ptr<autocommit_marker> _next;
  };
}