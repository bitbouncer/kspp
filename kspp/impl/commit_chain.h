#include <functional>
#include <memory>
#include <ctype.h>
#pragma once

namespace kspp {
class commit_chain
{
  public:

  class transaction_marker
  {
    public:
    transaction_marker(std::function <void(int64_t offset, int32_t ec)> callback)
      : _offset(-1)
      , _ec(0)
      , _cb(callback) {
    }
   
    ~transaction_marker() {
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

    void init(int64_t offset, std::shared_ptr<transaction_marker> next) {
      _offset = offset;
      _next = next;
    }
    
    private:
    int64_t                                          _offset;
    int32_t                                          _ec;
    std::function <void(int64_t offset, int32_t ec)> _cb;
    std::shared_ptr<transaction_marker>              _next;
  };

  public:
  commit_chain();

  void set_handler(std::function <void(int64_t offset, int32_t ec)> callback);

  size_t size() const { 
    return _size; 
  }

  std::shared_ptr<commit_chain::transaction_marker> create(int64_t offset);

  private:
  void handle_result(int64_t offset, int32_t ec);

  std::function <void(int64_t offset, int32_t ec)> _cb;
  std::shared_ptr<transaction_marker>              _next;
  size_t                                           _size;
};
};