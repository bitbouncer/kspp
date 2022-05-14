#include <memory>
#include <kspp/kspp.h>

#pragma once

// null sink - useful for testing
namespace kspp {
  template<class K, class V>
  class null_sink : public topic_sink<K, V> {
    static constexpr const char *PROCESSOR_NAME = "genric_sink";
  public:
    typedef std::function<void(std::shared_ptr<const krecord<K, V>> record)> handler;

    null_sink(std::shared_ptr<cluster_config> config, handler f = nullptr)
        : topic_sink<K, V>(), handler_(f) {
      this->add_metrics_label(KSPP_PROCESSOR_TYPE_TAG, "null_sink");
    }

    ~null_sink() override {
      this->flush();
    }

    void close() override {
    }

    std::string log_name() const override {
      return PROCESSOR_NAME;
    }

    size_t queue_size() const override {
      return event_consumer<K, V>::queue_size();
    }

    void flush() override {
      while (process(kspp::milliseconds_since_epoch()) > 0) { ; // noop
      }
    }

    bool eof() const override {
      return this->queue_.size();
    }

    size_t process(int64_t tick) override {
      size_t processed = 0;

      //forward up this timestamp
      while (this->queue_.next_event_time() <= tick) {
        auto r = this->queue_.pop_front_and_get();
        this->lag_.add_event_time(tick, r->event_time());
        ++(this->processed_count_);
        if (handler_)
          handler_(r->record());
        ++processed;
      }
      return processed;
    }

  protected:
    handler handler_;
  };
}