#include "mqtt_consumer.h"
#include <memory>
#include <strstream>
#include <thread>
#include <glog/logging.h>
#include <kspp/kspp.h>
#include <kspp/topology.h>
#pragma once

namespace kspp {
  class mqtt_source : public partition_source<std::string, std::string> {
  public:
    mqtt_source(std::shared_ptr<cluster_config> config, std::string mqtt_endpoint,
              mqtt::connect_options connect_options, int32_t partition) :
        partition_source<std::string, std::string>(nullptr, partition){
      impl_ = std::make_unique<mqtt_consumer>(mqtt_endpoint, connect_options);
      impl_->register_metrics(this);
      impl_->start();
    }

    ~mqtt_source() override {
      close();
    }

    std::string log_name() const override {
      return "mqtt_source";
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
      return impl_->eof();
    }

    void commit(bool flush) override {
      //impl_->commit(flush);
    }

    // TBD if we store last offset and end of stream offset we can use this...
    size_t queue_size() const override {
      return impl_->queue().size();
    }

    int64_t next_event_time() const override {
      return impl_->queue().next_event_time();
    }

    size_t process(int64_t tick) override {
      if (impl_->queue().size() == 0)
        return 0;
      size_t processed = 0;
      while (!impl_->queue().empty()) {
        auto p = impl_->queue().front();
        if (p == nullptr || p->event_time() > tick)
          return processed;
        impl_->queue().pop_front();
        this->send_to_sinks(p);
        ++(this->processed_count_);
        ++processed;
        this->lag_.add_event_time(tick, p->event_time());
      }
      return processed;
    }

    std::string topic() const override {
      return "mqtt-dummy";
      //return impl_->logical_name();
    }

  protected:
    std::unique_ptr<mqtt_consumer> impl_;
  };
}


