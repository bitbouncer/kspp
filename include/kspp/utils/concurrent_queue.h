#include <chrono>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <queue>

#pragma once

template<typename T>
class concurrent_queue {
public:
  concurrent_queue() = default;

  concurrent_queue(const concurrent_queue &) = delete;

  concurrent_queue &operator=(const concurrent_queue &) = delete;

  void push(const T &item) {
    std::unique_lock<std::mutex> lk(cv_m_);
    queue_.push(item);
    lk.unlock();
    cv_.notify_one();
  }

  inline bool empty() const {
    std::unique_lock<std::mutex> lk(cv_m_);
    return queue_.empty();
  }

  T pop() {
    std::unique_lock<std::mutex> lk(cv_m_);
    while (queue_.empty())
      cv_.wait(lk);
    auto val = queue_.front();
    queue_.pop();
    return val;
  }

  void pop(T &item) {
    std::unique_lock<std::mutex> lk(cv_m_);
    while (queue_.empty())
      cv_.wait(lk);
    item = queue_.front();
    queue_.pop();
  }

  bool try_pop(T &item) {
    std::unique_lock<std::mutex> lk(cv_m_);
    if (queue_.empty())
      return false;
    item = queue_.front();
    queue_.pop();
    return true;
  }

  template<class Rep, class Period>
  bool try_pop(T &item, const std::chrono::duration<Rep, Period> &rel_time) {
    std::unique_lock<std::mutex> lk(cv_m_);
    while (queue_.empty()) {
      if (cv_.wait_for(lk, rel_time) == std::cv_status::timeout)
        return false;
    }
    item = queue_.front();
    queue_.pop();
    return true;
  }

private:
  std::queue<T> queue_;
  std::mutex cv_m_;
  std::condition_variable cv_;
};