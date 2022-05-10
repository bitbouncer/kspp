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
    mqtt_sink(std::shared_ptr<cluster_config> config, std::string mqtt_endpoint, mqtt::connect_options connect_options){
      _impl = std::make_unique<mqtt_producer>(mqtt_endpoint, connect_options);
      _impl->register_metrics(this);
      _impl->start();
    }

    ~mqtt_sink() override {
      close();
    }

    std::string log_name() const override{
      return "mqtt_sink";
    }

    bool good() const {
      return _impl->good();
    }

    void register_metrics(kspp::processor* parent){
      _impl->register_metrics(parent);
    }

    void close() override {
      if (!_exit) {
        _exit = true;
      }
      _impl->close();
    }

    bool eof() const override {
      return this->_queue.size()==0 && _impl->eof();
    }

    size_t queue_size() const override {
      return this->_queue.size();
    }

    size_t outbound_queue_len() const override {
      return this->_impl->queue_size();
    }

    int64_t next_event_time() const override {
      return this->_queue.next_event_time();
    }

    size_t process(int64_t tick) override {
      if (this->_queue.empty())
        return 0;
      size_t processed=0;
      while(!this->_queue.empty()) {
        auto p = this->_queue.front();
        if (p==nullptr || p->event_time() > tick)
          return processed;
        this->_queue.pop_front();
        _impl->insert(p);
        ++(this->_processed_count);
        ++processed;
        this->_lag.add_event_time(tick, p->event_time());
      }
      return processed;
    }

    std::string topic() const override {
      return _impl->topic();
    }

    void poll(int timeout) override {
      _impl->poll();
    }

    void flush() override {
      while (!eof()) {
        process(kspp::milliseconds_since_epoch());
        poll(0);
        std::this_thread::sleep_for(std::chrono::milliseconds(10)); // TODO the deletable messages should be deleted when poill gets called an not from background thread 3rd queue is needed...
      }

      while (true) {
        int ec=0; // TODO fixme
        //auto ec = _impl.flush(1000);
        if (ec == 0)
          break;
      }
    }

  protected:
    bool _exit;
    std::unique_ptr<mqtt_producer> _impl;
  };
}


