#include <deque>
#include <memory>
#include <kspp/utils/spinlock.h>
#pragma once

namespace kspp {

/** 
  normal use is single producer / single consumer 
  but for topic sinks this will be multiproducer / single consumer
  TBD optimize for this usecase
*/
  template<class KEVENT>
  class event_queue {
  public:
    event_queue() {
    }

    inline size_t size() const {
      spinlock::scoped_lock xxx(_spinlock);
      return _queue.size();
    }

    inline void push_back(std::shared_ptr<KEVENT> p) {
      spinlock::scoped_lock xxx(_spinlock);
      _queue.push_back(p);
    }

    inline std::shared_ptr<KEVENT> front() {
      spinlock::scoped_lock xxx(_spinlock);
      return _queue.front();
    }

    inline void pop_front() {
      spinlock::scoped_lock xxx(_spinlock);
      _queue.pop_front();
    }

  private:
    std::deque<std::shared_ptr<KEVENT>> _queue;
    mutable spinlock _spinlock;
  };

}