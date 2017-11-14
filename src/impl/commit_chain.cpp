#include <kspp/impl/commit_chain.h>
#include <glog/logging.h>

namespace kspp {
  commit_chain::commit_chain(std::string topic, int32_t partition)
          : _topic(topic), _partition(partition), _size(0), _last_good_offset(-1), _first_ec(0),
            _next(std::make_shared<autocommit_marker>([this](int64_t offset, int32_t ec) {
              handle_result(offset, ec);
            })) {
  }

//commit_chain::commit_chain()
//  : _cb(nullptr)
//  , _size(0)
//  , _next(autocommit_marker::initialize([this](int64_t offset, int32_t ec) { handle_result(offset, ec); })) {
//}

/*
void commit_chain::set_handler(std::function <void(int64_t offset, int32_t ec)> callback) {
  _cb = callback;
}
*/

//std::shared_ptr<commit_chain::autocommit_marker> commit_chain::create(int64_t offset) {
//  ++_size;
//
//  auto next = autocommit_marker::initialize([this](int64_t offset, int32_t ec) { handle_result(offset, ec); });
//
//  _next->init(offset, next);
//  auto res = _next;
//  _next = next;
//  return res;
//}

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

// tbd we might want to have several error handling algoritms
// fatal as below or just a warning and skip?
  void commit_chain::handle_result(int64_t offset, int32_t ec) {
    if (offset >= 0) { // the "next" object with -1 is invalid
      if (_first_ec) // we never continue after first failure
        return;
      if (!ec) {
        --_size;
        _last_good_offset = offset;
      } else {
        _first_ec = ec;
        LOG(FATAL) << "commit_chain failed, topic " << _topic << ":" << _partition
                   << ", failure at offset:" << offset << ", ec:" << ec;
      }
    }
  }
}