#include <functional>
#include <memory>
#include <ctype.h>
#pragma once

namespace kspp {
class commit_chain
{
  public:

  class autocommit_marker
  {
    public:
      autocommit_marker(std::function <void(int64_t offset, int32_t ec)> callback)
      : _offset(-1)
      , _ec(0)
      , _cb(callback) {
    }
   
    /*
    public:
    inline static std::shared_ptr<autocommit_marker> initialize(std::function <void(int64_t offset, int32_t ec)> callback) {
      return std::make_shared<autocommit_marker>(callback);
    }
    */

    ~autocommit_marker() {
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

    void init(int64_t offset, std::shared_ptr<autocommit_marker> next) {
      _offset = offset;
      _next = next;
    }
    
    private:
    int64_t                                          _offset;
    int32_t                                          _ec;
    std::function <void(int64_t offset, int32_t ec)> _cb;
    std::shared_ptr<autocommit_marker>               _next;
  };

  public:
  commit_chain(std::string topic, int32_t partition);

  //void set_handler(std::function <void(int64_t offset, int32_t ec)> callback);
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

  const std::string                  _topic;
  const int32_t                      _partition;
  size_t                             _size;
  int64_t                            _last_good_offset;
  int32_t                            _first_ec;
  std::shared_ptr<autocommit_marker> _next;
};
};