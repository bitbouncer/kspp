#include <chrono>
#include <memory>
#include <mqtt/async_client.h>
#include <kspp/internal/queue.h>
#include <kspp/internal/commit_chain.h>
#include <kspp/topology.h>
#pragma once

namespace kspp {
class mqtt_consumer {
public:
  mqtt_consumer(std::string mqtt_endpoint,
                mqtt::connect_options connect_options);

  ~mqtt_consumer();

  void start() {
    start_ = true;
  }

  inline bool good() const {
    return good_;
  }

  void close();

  inline bool eof() const {
    return (incomming_msg_.size() == 0) && eof_;
  }


  void register_metrics(kspp::processor *parent);

  inline event_queue<std::string, std::string>& queue(){
    return incomming_msg_;
  };

  inline const event_queue<std::string, std::string>& queue() const {
    return incomming_msg_;
  };


private:
  class callback : public virtual mqtt::callback {
  public:
    callback(class mqtt_consumer *parent) : parent_(parent) {}

  protected:
    // (Re)connection success
    void connected(const std::string &cause) override {
      // do nothing
    }

    // Callback for when the connection is lost.
    void connection_lost(const std::string &cause) override {
      parent_->on_connection_lost();
    }

    // when a message arrives.
    void message_arrived(mqtt::const_message_ptr msg) override {
      // do nothing - should never happen
    }

    // what does this one do?? - doe not seem to be called - we use publish listener instead
    void delivery_complete(mqtt::delivery_token_ptr token) override {
      // do nothing - is never called???
    }

  private:
    mqtt_consumer *parent_ = nullptr;
  };

  void on_connection_lost(); // from callback



  void thread();

  bool exit_ = false;
  bool start_ = false;
  bool good_ = true;
  bool eof_=false;
  bool closed_ = false;
  bool connected_ = false; // ??

  std::unique_ptr<mqtt::async_client> client_;
  std::string mqtt_endpoint_;
  mqtt::connect_options connect_options_;
  callback action_listener_;

  //const int32_t partition_;
  const std::string stream_name_;
  //std::shared_ptr<offset_storage> offset_storage_;
  event_queue<std::string, std::string> incomming_msg_;
  // move

  metric_counter connection_errors_;
  metric_counter msg_cnt_;
  metric_counter msg_bytes_;

  std::thread thread_;
};
}
