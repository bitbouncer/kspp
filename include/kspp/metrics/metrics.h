#include <string>
#include <chrono>
#include <functional>
#include "metric20_key_t.h"
#pragma once

#define KSPP_KEY_TYPE_TAG "key_type"
#define KSPP_VALUE_TYPE_TAG "value_type"
#define KSPP_PROCESSOR_TYPE_TAG "processor_type"
#define KSPP_PROCESSOR_INSTANCE_NAME_TAG "processor_instance_name"
#define KSPP_PARTITION_TAG "partition"
#define KSPP_TOPIC_TAG "topic"

namespace kspp {

  inline metrics20::avro::metrics20_key_tags_t make_metrics_tag(std::string k, std::string v){
    metrics20::avro::metrics20_key_tags_t t;
    t.key=k;
    t.value=v;
    return t;
  }

  struct metric {
    enum mtype { RATE, COUNT, GAUGE, COUNTER, TIMESTAMP }; // http://metrics20.org/spec/

    metric(std::string what, mtype mt, std::string unit)
        : _name("kspp." + what) {
      add_tag("framework", "kspp");
      add_tag("what", what);
      switch (mt) {
        case RATE: add_tag("mtype", "rate"); break;
        case COUNT: add_tag("mtype", "count"); break;
        case GAUGE: add_tag("mtype", "gauge"); break;
        case COUNTER: add_tag("mtype", "counter"); break;
        case TIMESTAMP: add_tag("mtype", "timestamp"); break;
      }
      add_tag("unit", unit);
    }

    virtual int64_t value() const = 0;

    inline std::string name() const {
      return _name;
    }

    void finalize_tags()
    {
      _derived_tags.clear();
      for (std::map<std::string, std::string>::const_iterator i =_real_tags.begin(); i!=_real_tags.end(); ++i)
      {
        _derived_tags += i->first + "=" + i->second;
        if (std::next(i) != _real_tags.end())
          _derived_tags += ",";
      }
    }

    /*void set_tags(std::string s) {
      _tags = s;
    }
     */

    inline std::string tags() const {
      return _derived_tags;
    }

    void add_tag(std::string key, std::string val)
    {
      _real_tags[key]=val;
    }

    /*
     * static bool tag_order_fn (const metrics20::avro::metrics20_key_tags_t& i,const metrics20::avro::metrics20_key_tags_t& j) { return (i.key<j.key); }

    inline void sort_tags() {
      std::sort(_real_tags.begin(), _real_tags.end(), tag_order_fn);
    }
     */

    std::string _name; // what
    std::string _derived_tags;
    std::map<std::string, std::string> _real_tags;
  };

  struct metric_counter : public metric {
    metric_counter(std::string what, std::string unit)
        : metric(what, COUNTER, unit),
          _value(0) {
    }

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
    metric_average(std::string what, std::string unit)
        : metric(what, GAUGE, unit), _sum(0), _count(0) {}

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
    metric_lag()
        : metric("streaming_lag", GAUGE, "ms"), _lag(-1) {}

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

    metric_evaluator(std::string what,  mtype mt, std::string unit, evaluator f)
        : metric(what, mt, unit), _f(f) {}

    virtual int64_t value() const {
      return _f();
    }

  private:
    evaluator _f;
  };
}
