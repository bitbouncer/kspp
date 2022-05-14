#include "mqtt_producer.h"
#include <memory>
#include <strstream>
#include <thread>
#include <glog/logging.h>
#include <kspp/kspp.h>
#include <kspp/topology.h>

#pragma once

namespace kspp {
  class mqtt_sink : public topic_sink<std::string, std::string> {
  public:
    mqtt_sink(std::shared_ptr<cluster_config> config, std::string mqtt_endpoint,
              mqtt::connect_options connect_options) {
      impl_ = std::make_unique<mqtt_producer>(mqtt_endpoint, connect_options);
      impl_->register_metrics(this);
      impl_->start();
    }

    ~mqtt_sink() override {
      close();
    }

    std::string log_name() const override {
      return "mqtt_sink";
    }

    bool good() const {
      return impl_->good();
    }

    void register_metrics(kspp::processor *parent) {
      impl_->register_metrics(parent);
    }

    void close() override {
      impl_->close();
    }

    bool eof() const override {
      return this->queue_.size() == 0 && impl_->eof();
    }

    size_t queue_size() const override {
      return this->queue_.size();
    }

    size_t outbound_queue_len() const override {
      return this->impl_->queue_size();
    }

    int64_t next_event_time() const override {
      return this->queue_.next_event_time();
    }

    size_t process(int64_t tick) override {
      if (this->queue_.empty())
        return 0;
      size_t processed = 0;
      while (!this->queue_.empty()) {
        auto p = this->queue_.front();
        if (p == nullptr || p->event_time() > tick)
          return processed;
        this->queue_.pop_front();
        impl_->insert(p);
        ++(this->processed_count_);
        ++processed;
        this->lag_.add_event_time(tick, p->event_time());
      }
      return processed;
    }

    std::string topic() const override {
      return impl_->topic();
    }

    void poll(int timeout) override {
      impl_->poll();
    }

    void flush() override {
      while (!eof()) {
        process(kspp::milliseconds_since_epoch());
        poll(0);
        std::this_thread::sleep_for(std::chrono::milliseconds(
            10)); // TODO the deletable messages should be deleted when poill gets called an not from background thread 3rd queue is needed...
      }

      while (true) {
        int ec = 0; // TODO fixme
        //auto ec = _impl.flush(1000);
        if (ec == 0)
          break;
      }
    }

  protected:
    std::unique_ptr<mqtt_producer> impl_;
  };
}


