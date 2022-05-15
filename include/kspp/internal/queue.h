#include <deque>
#include <memory>
#include <cstdint>
#include <kspp/utils/spinlock.h>

#pragma once

namespace kspp {
  template<class ITEM>
  class queue {
  public:
    queue() : empty_(true) {
    }

    inline size_t size() const {
      spinlock::scoped_lock xxx(spinlock_);
      return queue_.size();
    }

    inline bool empty() const {
      return empty_;
    }

    inline void push_back(ITEM i) {
      spinlock::scoped_lock xxx(spinlock_);
      {
        empty_ = false;
        queue_.push_back(i);
      }
    }

    inline void push_front(ITEM i) {
      spinlock::scoped_lock xxx(spinlock_);
      {
        empty_ = false;
        queue_.push_front(i);
      }
    }

    inline ITEM front() {
      spinlock::scoped_lock xxx(spinlock_);
      return queue_.front();
    }

    inline void pop_front() {
      spinlock::scoped_lock xxx(spinlock_);
      {
        queue_.pop_front();
        if (queue_.size() == 0)
          empty_ = true;
      }
    }

    inline ITEM pop_and_get() {
      spinlock::scoped_lock xxx(spinlock_);
      {
        auto p = queue_.front();
        queue_.pop_front();

        if (queue_.size() == 0)
          empty_ = true;
        return p;
      }
    }

  private:
    std::deque<ITEM> queue_;
    bool empty_;
    mutable spinlock spinlock_;
  };

}