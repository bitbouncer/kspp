#include <assert.h>
#include <memory>
#include <functional>
#include <sstream>
#include <kspp/kspp.h>
#include <kspp/serdes/text_serdes.h>

#pragma once

namespace kspp {
  template<class K, class V>
  void stream_sink_print(std::shared_ptr<kevent<K, V>> ev, std::ostream &os_, kspp::text_serdes *_codec) {
    os_ << "ts: " << ev->event_time() << "  ";
    _codec->encode(ev->record()->key(), os_);
    os_ << ":";
    if (ev->record()->value())
      _codec->encode(*ev->record()->value(), os_);
    else
      os_ << "<nullptr>";
    os_ << std::endl;
  }

  template<class V>
  void stream_sink_print(std::shared_ptr<kevent<void, V>> ev, std::ostream &os_, kspp::text_serdes *_codec) {
    os_ << "ts: " << ev->event_time() << "  ";
    _codec->encode(*ev->record()->value(), os_);
    os_ << std::endl;
  }

  template<class K>
  void stream_sink_print(std::shared_ptr<kevent<K, void>> ev, std::ostream &os_, kspp::text_serdes *_codec) {
    os_ << "ts: " << ev->event_time() << "  ";
    _codec->encode(ev->record()->key(), os_);
    os_ << std::endl;
  }

  template<class K, class V>
  class stream_sink : public partition_sink<K, V> {
    static constexpr const char *PROCESSOR_NAME = "stream_sink";
  public:
    enum {
      MAX_KEY_SIZE = 1000
    };

    stream_sink(std::shared_ptr<cluster_config> config, std::shared_ptr<partition_source<K, V>> source,
                std::ostream *os)
        : partition_sink<K, V>(source->partition()), os_(*os), codec_(std::make_shared<kspp::text_serdes>()) {
      source->add_sink(this);
      this->add_metrics_label(KSPP_PROCESSOR_TYPE_TAG, "stream_sink");
    }

    ~stream_sink() override {
      this->flush();;
    }

    std::string log_name() const override {
      return PROCESSOR_NAME;
    }

    size_t queue_size() const override {
      return event_consumer<K, V>::queue_size();
    }

    void commit(bool flush) override {
      // noop
    }

    size_t process(int64_t tick) override {
      size_t processed = 0;
      while (this->queue_.size()) {
        auto ev = this->queue_.front();
        stream_sink_print(ev, os_, codec_.get());
        this->queue_.pop_front();
        ++(this->processed_count_);
        this->lag_.add_event_time(kspp::milliseconds_since_epoch(), ev->event_time()); // move outside loop
        ++processed;
      }
      return processed;
    }

    std::ostream &os_;
    std::shared_ptr<kspp::text_serdes> codec_;
  };
}