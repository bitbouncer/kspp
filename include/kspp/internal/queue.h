#include <deque>
#include <memory>
#include <cstdint>
#include <kspp/utils/spinlock.h>

#pragma once

namespace kspp {
  template<class ITEM>
  class queue {
  public:
    queue() : _empty(true) {
    }

    inline size_t size() const {
      spinlock::scoped_lock xxx(_spinlock);
      return _queue.size();
    }

    inline bool empty() const {
      return _empty;
    }

    inline void push_back(ITEM i) {
      spinlock::scoped_lock xxx(_spinlock);
      {
        _empty = false;
        _queue.push_back(i);
      }
    }

    inline void push_front(ITEM i) {
      spinlock::scoped_lock xxx(_spinlock);
      {
        _empty = false;
        _queue.push_front(i);
      }
    }

    inline ITEM front() {
      spinlock::scoped_lock xxx(_spinlock);
      return _queue.front();
    }

    inline void pop_front() {
      spinlock::scoped_lock xxx(_spinlock);
      {
        _queue.pop_front();
        if (_queue.size() == 0)
          _empty = true;
      }
    }

    inline ITEM pop_and_get() {
      spinlock::scoped_lock xxx(_spinlock);
      {
        auto p = _queue.front();
        _queue.pop_front();

        if (_queue.size() == 0)
          _empty = true;
        return p;
      }
    }

  private:
    std::deque<ITEM> _queue;
    bool _empty;
    mutable spinlock _spinlock;
  };

}