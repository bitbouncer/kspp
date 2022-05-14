#include "mqtt_producer.h"

#define KEEP_ALIVE_INTERVALL 1000
#define MAX_PAYLOAD_IN_FLIGHT 100
// should we have this at all????
const int MAX_BUFFERED_MSGS = 120;  // 120 * 5sec => 10min off-line buffering
const std::string PERSIST_DIR{"data-persist"};

namespace kspp {
  mqtt_producer::mqtt_producer(std::string mqtt_endpoint, mqtt::connect_options connect_options)
      : mqtt_endpoint_(mqtt_endpoint)
        , connect_options_(connect_options)
        , action_listener_(this)
        , publish_listener_(this)
        , connection_errors_("connection_errors", "msg")
        , msg_cnt_("inserted", "msg")
        , msg_bytes_("bytes_sent", "bytes")
        , thread_([this]() { thread(); }) {
    // override required params to be sure
    connect_options_.set_automatic_reconnect(true);
    client_ = std::make_unique<mqtt::async_client>(mqtt_endpoint_, "", MAX_BUFFERED_MSGS,
                                                   PERSIST_DIR); // todo check clientid & persisted messages - should be disabled???
    client_->set_callback(action_listener_);
  }

  mqtt_producer::~mqtt_producer() {
    exit_ = true;
    close();
    thread_.join();
    client_.reset(nullptr);
  }

  void mqtt_producer::register_metrics(kspp::processor *parent) {
    parent->add_metric(&connection_errors_);
    parent->add_metric(&msg_cnt_);
    parent->add_metric(&msg_bytes_);
  }

  void mqtt_producer::close() {
    if (closed_)
      return;
    closed_ = true;
    if (client_ && client_->is_connected())
      client_->disconnect();
    LOG(INFO) << "mqtt_producer closed - written " << (int64_t) msg_cnt_.value() << " messages ("
              << (int64_t) msg_bytes_.value() << " bytes)";
    good_ = false;
    connected_ = false;
  }

  void mqtt_producer::poll() {
    while (!done_.empty()) {
      done_.pop_front();
    }
  }

  void mqtt_producer::on_connection_lost() {
    LOG(INFO) << "mqtt_producer::on_connection_lost, moving " << pending_msg_.size() << " messages back to send queue";
    ++connection_errors_;
    while (!pending_msg_.empty()) {
      auto p = pending_msg_.back();
      pending_msg_.pop_back();
      incomming_msg_.push_front(p);
    }
  }

  // TODO do we have a guarantee that messages gets acked in order???
  void mqtt_producer::on_publish_success() {
    auto p = pending_msg_.pop_front_and_get();
    ++msg_cnt_;
    msg_bytes_ += p->record()->key().size() + p->record()->value()->size();
    //LOG(INFO) << "on_publish_success lag " << kspp::milliseconds_since_epoch() - p->record()->event_time() << " ms";
    done_.push_back(p);
  }

  void mqtt_producer::thread() {
    while (!start_ && !exit_) {
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    while (!exit_) {
      try {
        LOG(INFO) << "Initial connect to server '" << mqtt_endpoint_ << "'...";
        client_->connect(connect_options_)->wait();
        LOG(INFO) << "initial connection successful - starting operations";
        break;
      } catch (std::exception &e) {
        LOG(INFO) << "connect failed: " << e.what();
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(5000));
    }

    while (!exit_) {
      if (!client_->is_connected()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1000));
        if (!pending_msg_.empty())
          LOG(INFO) << "mqtt_producer::thread() - connection lost - moving " << pending_msg_.size()
                    << " messages back to send queue";
        while (!pending_msg_.empty()) {
          auto p = pending_msg_.back();
          pending_msg_.pop_back();
          incomming_msg_.push_front(p);
        }
        continue;
      }

      while (!exit_ && !incomming_msg_.empty()) {
        auto msg = incomming_msg_.pop_front_and_get();
        if (msg->record()->value() == nullptr) {
          DLOG(INFO) << "skipping delete";
          done_.push_back(msg);
          continue;
        }
        pending_msg_.push_back(msg);
        //LOG(INFO) << "sending key " << msg->record()->key() << " value: " << *msg->record()->value() << " lag " << kspp::milliseconds_since_epoch() - msg->record()->event_time() << " ms";
        auto pubmsg = mqtt::make_message(msg->record()->key(), *msg->record()->value());
        client_->publish(pubmsg, nullptr, publish_listener_);
        continue;
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    LOG(INFO) << "exiting thread";
  }
}