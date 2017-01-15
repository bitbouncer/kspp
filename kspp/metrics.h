#include <string>
#include <chrono>

#pragma once

namespace kspp {
  struct metrics
  {
    metrics(std::string s)
      : _simple_name(s)
      , _logged_name(s) {
    }

    virtual int64_t value() const = 0;
    
    inline std::string name() {
      return _logged_name;
    }

    std::string _simple_name;
    std::string _logged_name;
  };
  
  struct metrics_counter : public metrics
  {
    metrics_counter(std::string s)
      : metrics(s)
      , _value(0) {
    }

    virtual int64_t value() const {
      return _value;
    }

    inline metrics_counter& operator=(int64_t v) {
      _value = v;
      return *this;
    }

    inline metrics_counter& operator++() {
      _value++;
      return *this;
    }

    inline metrics_counter& operator+=(int64_t v) {
      _value = _value + v;
      return *this;
    }

    inline metrics_counter& operator--() {
      _value--;
      return *this;
    }

    inline metrics_counter& operator-=(int64_t v) {
      _value = _value - v;
      return *this;
    }
    int64_t _value;
  };

  struct metrics_average : public metrics
  {
    metrics_average(std::string s)
      : metrics(s)
      , _sum(0)
      , _count(0) 
    {}

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

  struct metrics_lag : public metrics
  {
    // TBD is this fast enough???
    static inline int64_t milliseconds_since_epoch() {
      return std::chrono::duration_cast<std::chrono::milliseconds>
        (std::chrono::system_clock::now().time_since_epoch()).count();
    }

    metrics_lag()
      : metrics("lag")
      , _lag(-1){
    }

    inline void add_event_time(int64_t event_time) {
      if (event_time > 0)
        _lag = milliseconds_since_epoch() - event_time; 
    }

    virtual int64_t value() const {
      return _lag;
    }
  private:
    int64_t _lag;
  };
};