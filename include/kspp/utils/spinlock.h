#include <atomic>
#include <thread>
#include <mutex>
#pragma once

namespace kspp {
//class spinlock
//{
//  public:
//  using scoped_lock = std::unique_lock<spinlock>;
//
//  inline spinlock() : _lock(ATOMIC_FLAG_INIT) {
//  }
//
//  inline void lock() {
//    while (true) {
//      for (int32_t i = 0; i < 10000; ++i) {
//        if (!_lock.test_and_set(std::memory_order_acquire)) {
//          return;
//        }
//      }
//      std::this_thread::yield();
//    }
//  }
//
//  inline bool try_lock() {
//    return !_lock.test_and_set(std::memory_order_acquire);
//  }
//
//  inline void unlock() {
//    _lock.clear(std::memory_order_release);
//  }
//
//  private:
//  std::atomic_flag _lock;
//  spinlock(spinlock const&) = delete;
//  spinlock & operator=(spinlock const&) = delete;
//};

  class spinlock
  {
    std::atomic_flag locked = ATOMIC_FLAG_INIT ;
  public:
    using scoped_lock = std::unique_lock<spinlock>;

    spinlock() {}

    inline void lock() {
      while (locked.test_and_set(std::memory_order_acquire)) { ; }
    }

    inline void unlock() {
      locked.clear(std::memory_order_release);
    }
  private:
    spinlock(spinlock const&) = delete;
    spinlock & operator=(spinlock const&) = delete;
  };

} // namespace
