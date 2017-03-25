#include "commit_chain.h"

namespace kspp {
commit_chain::commit_chain()
  : _cb(nullptr)
  , _size(0)
  , _next(std::make_shared<autocommit_marker>([this](int64_t offset, int32_t ec) {
  handle_result(offset, ec);
})) {
}

void commit_chain::set_handler(std::function <void(int64_t offset, int32_t ec)> callback) {
  _cb = callback;
}


std::shared_ptr<commit_chain::autocommit_marker> commit_chain::create(int64_t offset) {
  ++_size;

  auto next = std::make_shared<autocommit_marker>([this](int64_t offset, int32_t ec) {
    handle_result(offset, ec);
  });

  _next->init(offset, next);
  auto res = _next;
  _next = next;
  return res;
}

void commit_chain::handle_result(int64_t offset, int32_t ec) {
  if (offset >= 0) { // the "next" object with -1 is invalid
    --_size;
    if (_cb)
      _cb(offset, ec);
  }
}
};