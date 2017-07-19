#include <atomic>
#include <mutex>
#include <thread>

#pragma once

namespace kspp {
// copy of csi-async to get rid of dependency
  class spinlock {
  public:
    using scoped_lock = std::unique_lock<spinlock>;

    inline spinlock() {
      _lock.clear();
    }

    inline void lock() {
      while (true) {
        for (int32_t i = 0; i < 10000; ++i) {
          if (!_lock.test_and_set(std::memory_order_acquire)) {
            return;
          }
        }
        std::this_thread::yield();
      }
    }

    inline bool try_lock() {
      return !_lock.test_and_set(std::memory_order_acquire);
    }

    inline void unlock() {
      _lock.clear(std::memory_order_release);
    }

  private:
    std::atomic_flag _lock;

    spinlock(spinlock const &) = delete;

    spinlock &operator=(spinlock const &) = delete;
  };

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