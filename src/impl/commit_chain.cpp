#include <kspp/impl/commit_chain.h>
#include <deque>
#include <glog/logging.h>

namespace kspp {

  static std::deque<std::shared_ptr<commit_chain::autocommit_marker>> s_pending_delete;

  commit_chain::autocommit_marker::~autocommit_marker() {
    //_cb(_offset, _ec);
    //now we could delete everting that is waiting for us - but if this is 100k objects the callstack is kind of large and we will segfault
    //so we check if we're the last holder of next - if so let someone else delete next object
    if (_next.use_count()==1){
      if (_next)
        s_pending_delete.push_back(_next);
      _next.reset();
    }
  }

  void autocommit_marker_gc() {
    while (s_pending_delete.size())
      s_pending_delete.pop_front();
  }

  commit_chain::commit_chain(std::string topic, int32_t partition)
          : _topic(topic), _partition(partition), _size(0), _last_good_offset(-1), _first_ec(0),
            _next(std::make_shared<autocommit_marker>([this](int64_t offset, int32_t ec) {
              handle_result(offset, ec);
            })) {
  }

  std::shared_ptr<commit_chain::autocommit_marker> commit_chain::create(int64_t offset) {
    spinlock::scoped_lock xxx(_spinlock);
    {
      ++_size;
    }
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
        spinlock::scoped_lock xxx(_spinlock);
        {
          --_size;
        }
        _last_good_offset = offset;
      } else {
        _first_ec = ec;
        LOG(FATAL) << "commit_chain failed, topic " << _topic << ":" << _partition
                   << ", failure at offset:" << offset << ", ec:" << ec;
      }
    }
  }
}