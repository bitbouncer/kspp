#include <glog/logging.h>
#include <kspp/avro/generic_avro.h>
#include <kspp/connect/generic_producer.h>
#include <kspp/kspp.h>
#include <kspp/topology.h>
#include <memory>
#include <strstream>
#include <thread>

#pragma once

namespace kspp {
class generic_avro_sink :
    public topic_sink<kspp::generic_avro, kspp::generic_avro> {
public:
  generic_avro_sink(
      std::shared_ptr<cluster_config> config,
      std::shared_ptr<generic_producer<kspp::generic_avro, kspp::generic_avro>>
          impl) :
      impl_(impl) {}

  ~generic_avro_sink() override { close(); }

  bool good() const { return impl_->good(); }

  void register_metrics(kspp::processor *parent) {
    impl_->register_metrics(parent);
  }

  void close() override {
    /*if (!exit_) {
      exit_ = true;
    }
    */
    impl_->close();
  }

  bool eof() const override { return this->queue_.size() == 0 && impl_->eof(); }

  size_t queue_size() const override { return this->queue_.size(); }

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

  std::string topic() const override { return impl_->topic(); }

  void poll(int timeout) override { impl_->poll(); }

  void flush() override {
    while (!eof()) {
      process(kspp::milliseconds_since_epoch());
      poll(0);
      std::this_thread::sleep_for(std::chrono::milliseconds(
          10)); // TODO the deletable messages should be deleted when poill gets
                // called an not from background thread 3rd queue is needed...
    }

    while (true) {
      int ec = 0; // TODO fixme
      // auto ec = _impl.flush(1000);
      if (ec == 0)
        break;
    }
  }

protected:
  // bool exit_= false;
  std::shared_ptr<generic_producer<kspp::generic_avro, kspp::generic_avro>>
      impl_;
};
} // namespace kspp
