#include <chrono>
#include <memory>
#include <kspp/internal/queue.h>
#include <kspp/internal/commit_chain.h>
#include <kspp/topology.h>
#include <mqtt/async_client.h>
#pragma once

namespace kspp {
  class mqtt_producer {
  public:
    mqtt_producer(std::string mqtt_endpoint, mqtt::connect_options connect_options);
    ~mqtt_producer();

    void start() {
      start_ = true;
    }

    void register_metrics(kspp::processor *parent);

    void close();

    inline bool good() const {
      return good_;
    }

    inline bool eof() const {
      return (_incomming_msg.empty() && _done.empty());
    }

    inline std::string topic() const {
      return "not available";
    }

    inline bool is_connected() const {
      return connected_;
    }

    inline void insert(std::shared_ptr<kevent<std::string, std::string>> p) {
      _incomming_msg.push_back(p);
    }

    void poll();

    inline size_t queue_size() const {
      auto sz0 = _incomming_msg.size();
      auto sz1 = _done.size();
      return sz0 + sz1;
    }

  private:
    class publish_listener : public virtual mqtt::iaction_listener{
    public:
      publish_listener(class mqtt_producer* parent)
          : parent_(parent){
      }

      void on_success(const mqtt::token& tok) override{
        parent_->on_publish_success();
      }

      void on_failure(const mqtt::token& tok) override{
        // do nothing?
      }

    private:
      mqtt_producer* parent_=nullptr;
    };

    class callback : public virtual mqtt::callback
    {
    public:
      callback(class mqtt_producer* parent)
          : parent_(parent) {
      }
    protected:
      // (Re)connection success
      void connected(const std::string& cause) override{
        // do nothing
      }

      // Callback for when the connection is lost.
      void connection_lost(const std::string& cause) override{
        parent_->on_connection_lost();
      }

      // when a message arrives.
      void message_arrived(mqtt::const_message_ptr msg) override{
        // do nothing - should never happen
      }

      // what does this one do?? - doe not seem to be called - we use publish listener instead
      void delivery_complete(mqtt::delivery_token_ptr token) override{
        // do nothing - is never called???
      }

    private:
      mqtt_producer* parent_=nullptr;
    };

    void on_connection_lost(); // from callback
    void on_publish_success(); // used from publish_listener

    //void initialize();

    void thread();

    bool exit_=false;
    bool start_=false;
    bool good_=true;
    bool closed_=false;
    bool connected_=false; // ??

    std::unique_ptr<mqtt::async_client> client_;
    std::string mqtt_endpoint_;
    mqtt::connect_options connect_options_;
    callback action_listener_;
    publish_listener publish_listener_;

    std::atomic_int cur_pending_messages_;
    std::atomic_int total_published_messages_;
    event_queue<std::string, std::string> _incomming_msg;
    event_queue<std::string, std::string> _pending_msg;
    event_queue<std::string, std::string> _done;  // waiting to be deleted in poll();
    size_t _max_items_in_insert;
    size_t _current_error_streak;
    metric_counter _connection_errors;
    metric_counter _msg_cnt;
    metric_counter _msg_bytes;

    std::thread thread_;
  };
}

