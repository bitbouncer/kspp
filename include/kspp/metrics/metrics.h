#include <string>
#include <chrono>
#include <functional>
#pragma once

namespace kspp {
  struct metric {
    metric(std::string s)
            : _name(s) {
    }

    virtual int64_t value() const = 0;

    inline std::string name() const {
      return _name;
    }

    void set_tags(std::string s) {
      _tags = s;
    }

    inline std::string tags() const {
      return _tags;
    }

    std::string _name;
    std::string _tags;
  };

  struct metric_counter : public metric {
    metric_counter(std::string s)
            : metric(s), _value(0) {}

    virtual int64_t value() const {
      return _value;
    }

    inline metric_counter &operator=(int64_t v) {
      _value = v;
      return *this;
    }

    inline metric_counter &operator++() {
      _value++;
      return *this;
    }

    inline metric_counter &operator+=(int64_t v) {
      _value = _value + v;
      return *this;
    }

    inline metric_counter &operator--() {
      _value--;
      return *this;
    }

    inline metric_counter &operator-=(int64_t v) {
      _value = _value - v;
      return *this;
    }

    int64_t _value;
  };

  struct metric_average : public metric {
    metric_average(std::string s)
            : metric(s), _sum(0), _count(0) {}

    void add_measurement(int64_t v) {
      _sum += v;
      ++_count;
    }

    virtual int64_t value() const {
      return _count ? _sum / _count : 0;
    }

    void clear() {
      _sum = 0;
      _count = 0;
    }

    int64_t _sum;
    int64_t _count;
  };

  struct metric_lag : public metric {
    // TBD is this fast enough???
    /*static inline int64_t milliseconds_since_epoch() {
      return std::chrono::duration_cast<std::chrono::milliseconds>
        (std::chrono::system_clock::now().time_since_epoch()).count();
    }*/

    metric_lag()
            : metric("lag"), _lag(-1) {}

    inline void add_event_time(int64_t tick, int64_t event_time) {
      if (event_time > 0)
        _lag = tick - event_time;
      else
        _lag = -1;
    }

    virtual int64_t value() const {
      return _lag;
    }

  private:
    int64_t _lag;
  };

  struct metric_evaluator : public metric {
    using evaluator = std::function<int64_t(void)>;

    metric_evaluator(std::string s, evaluator f)
            : metric(s), _f(f) {}

    virtual int64_t value() const {
      return _f();
    }

  private:
    evaluator _f;
  };
}