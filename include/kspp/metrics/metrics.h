#include <string>
#include <chrono>
#include <cmath>
#include <functional>
#include <prometheus/registry.h>
#include <prometheus/counter.h>
#include <prometheus/summary.h>
#include <prometheus/histogram.h>
#include <prometheus/detail/ckms_quantiles.h>

#pragma once

#define KSPP_KEY_TYPE_TAG "key_type"
#define KSPP_VALUE_TYPE_TAG "value_type"
#define KSPP_PROCESSOR_TYPE_TAG "processor_type"
#define KSPP_DESTINATION_HOST   "destination_host"
#define KSPP_PARTITION_TAG "partition"
#define KSPP_TOPIC_TAG "topic"

namespace kspp {
  struct metric {
    enum mtype {
      RATE, COUNT, GAUGE, COUNTER, TIMESTAMP, SUMMARY, HISTOGRAM
    }; // http://metrics20.org/spec/ and some prometheus

    metric(std::string what, mtype mt, std::string unit)
        : name_("kspp_" + what) {
      switch (mt) {
        case RATE:
          add_label("mtype", "rate");
          break;
        case COUNT:
          add_label("mtype", "count");
          break;
        case GAUGE:
          add_label("mtype", "gauge");
          break;
        case COUNTER:
          add_label("mtype", "counter");
          break;
        case TIMESTAMP:
          add_label("mtype", "timestamp");
          break;
        case SUMMARY:
          add_label("mtype", "summary");
          break;
        case HISTOGRAM:
          add_label("mtype", "histogram");
          break;
      }
      add_label("unit", unit);
    }

    virtual double value() const = 0;

    inline std::string name() const {
      return name_;
    }

    virtual void finalize_labels(std::shared_ptr<prometheus::Registry> registry) = 0;

    void add_label(std::string key, std::string val) {
      labels_[key] = val;
    }

  protected:
    std::string name_; // what
    std::map<std::string, std::string> labels_;
  };

  struct metric_counter : public metric {
    metric_counter(std::string what, std::string unit)
        : metric(what, COUNTER, unit) {
    }

    metric_counter(std::string what, std::string unit, const std::map<std::string, std::string> &labels,
                   std::shared_ptr<prometheus::Registry> registry)
        : metric(what, COUNTER, unit) {
      labels_.insert(labels.begin(), labels.end());
      auto &family = prometheus::BuildCounter().Name(name_).Register(*registry);
      counter_ = &family.Add(labels_);
    }

    void finalize_labels(std::shared_ptr<prometheus::Registry> registry) override {
      auto &family = prometheus::BuildCounter().Name(name_).Register(*registry);
      counter_ = &family.Add(labels_);
    }

    virtual double value() const {
      return counter_->Value();
    }

    inline metric_counter &operator++() {
      counter_->Increment();
      return *this;
    }

    inline metric_counter &operator+=(double v) {
      counter_->Increment(v);
      return *this;
    }
  private:
    prometheus::Counter *counter_ = nullptr;
  };

  struct metric_gauge : public metric {
    metric_gauge(std::string what, std::string unit)
        : metric(what, GAUGE, unit) {
    }

    metric_gauge(std::string what, std::string unit, const std::map<std::string, std::string> &labels,
                 std::shared_ptr<prometheus::Registry> registry)
        : metric(what, GAUGE, unit) {
      labels_.insert(labels.begin(), labels.end());
      auto &family = prometheus::BuildGauge().Name(name_).Register(*registry);
      gauge_ = &family.Add(labels_);
    }

    void finalize_labels(std::shared_ptr<prometheus::Registry> registry) override {
      auto &family = prometheus::BuildGauge().Name(name_).Register(*registry);
      gauge_ = &family.Add(labels_);
    }

    void set(double v) {
      gauge_->Set(v);
    }

    void incr(double v) {
      gauge_->Increment(v);
    }

    void decr(double v) {
      gauge_->Decrement(v);
    }

    virtual double value() const {
      return gauge_->Value();
    }

    void clear() {
      gauge_->Set(0);
    }
  private:
    prometheus::Gauge *gauge_ = nullptr;
  };

  struct metric_streaming_lag : public metric {
    metric_streaming_lag()
        : metric("streaming_lag", GAUGE, "ms") {
    }

    void finalize_labels(std::shared_ptr<prometheus::Registry> registry) override {
      auto &family = prometheus::BuildGauge().Name(name_).Register(*registry);
      gauge_ = &family.Add(labels_);
    }

    inline void add_event_time(int64_t tick, int64_t event_time) {
      if (event_time > 0)
        gauge_->Set(tick - event_time);
      else
        gauge_->Set(-1.0);
    }

    virtual double value() const {
      return gauge_->Value();
    }

  private:
    prometheus::Gauge *gauge_ = nullptr;
  };

  struct metric_summary : public metric {
    metric_summary(std::string what, std::string unit, const std::vector<float> &quantiles = {0.99})
        : metric(what, SUMMARY, unit), quantiles_(quantiles) {
      add_label("window", "60s");
    }

    metric_summary(std::string what, std::string unit, const std::map<std::string, std::string> &labels,
                   std::shared_ptr<prometheus::Registry> registry, const std::vector<float> &quantiles = {0.99})
        : metric(what, SUMMARY, unit) {
      labels_.insert(labels.begin(), labels.end());
      std::vector<prometheus::detail::CKMSQuantiles::Quantile> q;
      for (auto i: quantiles_)
        q.emplace_back(i, 0.05);
      auto &family = prometheus::BuildSummary().Name(name_).Register(*registry);
      summary_ = &family.Add(labels_, q, std::chrono::seconds{600}, 5);
    }

    void finalize_labels(std::shared_ptr<prometheus::Registry> registry) override {
      std::vector<prometheus::detail::CKMSQuantiles::Quantile> q;
      for (auto i: quantiles_)
        q.emplace_back(i, 0.05);
      auto &family = prometheus::BuildSummary().Name(name_).Register(*registry);
      summary_ = &family.Add(labels_, q, std::chrono::seconds{600}, 5);
    }

    inline void observe(double v) {
      summary_->Observe(v);
    }

    virtual double value() const {
      return NAN;
    }

  private:
    const std::vector<float> quantiles_;
    prometheus::Summary *summary_ = nullptr;
  };

  struct metric_histogram : public metric {
    metric_histogram(std::string what, std::string unit, const std::vector<double> &buckets)
        : metric(what, HISTOGRAM, unit), buckets_(buckets) {
    }

    void finalize_labels(std::shared_ptr<prometheus::Registry> registry) override {
      auto &family = prometheus::BuildHistogram().Name(name_).Register(*registry);
      histgram_ = &family.Add(labels_, buckets_);
    }

    inline void observe(double v) {
      histgram_->Observe(v);
    }

    virtual double value() const {
      return NAN;
    }

  private:
    const std::vector<double> buckets_;
    prometheus::Histogram *histgram_ = nullptr;
  };
}
