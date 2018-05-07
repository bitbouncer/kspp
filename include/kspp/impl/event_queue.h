#include <deque>
#include <memory>
#include <cstdint>
#include <kspp/utils/spinlock.h>
#include <kspp/kevent.h>
#pragma once

namespace kspp {

/** 
  normal use is single producer / single consumer 
  but for topic sinks this will be multiproducer / single consumer
  TBD optimize for this usecase
*/
  template<class K, class V>
  class event_queue {
  public:
    event_queue()
        : _next_event_time(INT64_MAX) {
    }

    inline size_t size() const {
      spinlock::scoped_lock xxx(_spinlock);
      return _queue.size();
    }

    inline int64_t next_event_time() const {
      return _next_event_time;
    }

    inline bool empty() const {
      return _next_event_time == INT64_MAX; // this is faster than locking..
    }

    inline void push_back(std::shared_ptr<kevent<K, V>> p) {
      if (p != nullptr && p.get() != nullptr) {
        spinlock::scoped_lock xxx(_spinlock);
        if (_queue.size() == 0)
          _next_event_time = p->event_time();
        _queue.push_back(p);
      }
    }

    inline std::shared_ptr<kevent<K, V>> front() {
      spinlock::scoped_lock xxx(_spinlock);
      return _queue.front();
    }

    inline void pop_front() {
      spinlock::scoped_lock xxx(_spinlock);
      {
        _queue.pop_front();
        if (_queue.size() == 0)
          _next_event_time = INT64_MAX;
        else
          _next_event_time = _queue[0]->event_time();
      }
    }

    inline std::shared_ptr<kevent<K, V>> pop_and_get() {
      if (empty())
        return nullptr;

      spinlock::scoped_lock xxx(_spinlock);
      {
        auto p = _queue.front();
        _queue.pop_front();

        if (_queue.size() == 0)
          _next_event_time = INT64_MAX;
        else
          _next_event_time = _queue[0]->event_time();
        return p;
      }
    }

  private:
    std::deque<std::shared_ptr<kevent<K, V>>> _queue;
    int64_t _next_event_time;
    mutable spinlock _spinlock;
  };

}