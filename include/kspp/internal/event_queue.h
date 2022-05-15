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
        : next_event_time_(INT64_MAX) {
    }

    inline size_t size() const {
      spinlock::scoped_lock xxx(spinlock_);
      { return queue_.size(); }
    }

    inline int64_t next_event_time() const {
      return next_event_time_;
    }

    inline bool empty() const {
      return next_event_time_ == INT64_MAX; // this is faster than locking..
    }

    //normal usage
    inline void push_back(std::shared_ptr<kevent<K, V>> p) {
      if (p) {
        spinlock::scoped_lock xxx(spinlock_);
        {
          if (queue_.size() == 0)
            next_event_time_ = p->event_time();
          queue_.push_back(p);
        }
      }
    }

    // used for error handling
    inline void push_front(std::shared_ptr<kevent<K, V>> p) {
      if (p) {
        spinlock::scoped_lock xxx(spinlock_);
        {
          next_event_time_ = p->event_time();
          queue_.push_front(p);
        }
      }
    }


    inline std::shared_ptr<kevent<K, V>> front() {
      spinlock::scoped_lock xxx(spinlock_);
      return queue_.front();
    }

    inline std::shared_ptr<kevent<K, V>> back() {
      spinlock::scoped_lock xxx(spinlock_);
      return queue_.back();
    }

    inline void pop_front() {
      spinlock::scoped_lock xxx(spinlock_);
      {
        queue_[0].reset();
        queue_.pop_front();
        if (queue_.size() == 0)
          next_event_time_ = INT64_MAX;
        else
          next_event_time_ = queue_[0]->event_time();
      }
    }

    // used for erro handling
    inline void pop_back() {
      spinlock::scoped_lock xxx(spinlock_);
      {
        queue_[queue_.size() - 1].reset();
        queue_.pop_back();
        if (queue_.size() == 0)
          next_event_time_ = INT64_MAX;
      }
    }


    inline std::shared_ptr<kevent<K, V>> pop_front_and_get() {
      if (empty())
        return nullptr;

      spinlock::scoped_lock xxx(spinlock_);
      {
        auto p = queue_.front();
        queue_[0].reset();
        queue_.pop_front();

        if (queue_.size() == 0)
          next_event_time_ = INT64_MAX;
        else
          next_event_time_ = queue_[0]->event_time();
        return p;
      }
    }

  private:
    std::deque<std::shared_ptr<kevent<K, V>>> queue_;
    int64_t next_event_time_;
    mutable spinlock spinlock_;
  };

}