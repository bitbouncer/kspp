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
          : event_done_marker(callback) {
      }

      ~autocommit_marker() override;

      void init(int64_t offset, std::shared_ptr<autocommit_marker> next) {
        event_done_marker::init(offset);
        //_offset = offset;
        next_ = next;
      }

    private:
      std::shared_ptr<autocommit_marker> next_;
    };

  public:
    commit_chain(std::string topic, int32_t partition);

    std::shared_ptr<commit_chain::autocommit_marker> create(int64_t offset);

    // nr of outstanding requests
    inline size_t size() const {
      return size_;
    }

    inline int64_t last_good_offset() const {
      return last_good_offset_;
    }

    // first error code
    inline int32_t first_ec() const {
      return first_ec_;
    }

  private:
    void handle_result(int64_t offset, int32_t ec);

    const std::string topic_;
    const int32_t partition_;
    mutable spinlock spinlock_;
    volatile size_t size_;
    int64_t last_good_offset_;
    int32_t first_ec_;
    std::shared_ptr<autocommit_marker> next_;
  };
}