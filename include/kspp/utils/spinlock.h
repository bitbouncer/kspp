#include <atomic>
#include <thread>
#include <mutex>

#pragma once

namespace kspp {
  class spinlock {
    std::atomic_flag locked = ATOMIC_FLAG_INIT;
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
    spinlock(spinlock const &) = delete;

    spinlock &operator=(spinlock const &) = delete;
  };

} // namespace
