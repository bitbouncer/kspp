#include <kspp/connect/mqtt/mqtt_producer.h>

#define KEEP_ALIVE_INTERVALL 1000
#define MAX_PAYLOAD_IN_FLIGHT 100
// should we have this at all????
const int MAX_BUFFERED_MSGS = 120;	// 120 * 5sec => 10min off-line buffering
const std::string PERSIST_DIR { "data-persist" };

namespace kspp{
  mqtt_producer::mqtt_producer(std::string mqtt_endpoint, mqtt::connect_options connect_options)
      : mqtt_endpoint_(mqtt_endpoint)
        , connect_options_(connect_options)
        , action_listener_(this)
        , publish_listener_(this)
        , _connection_errors("connection_errors", "msg")
        , _msg_cnt("inserted", "msg")
        , _msg_bytes("bytes_sent", "bytes")
        , thread_([this](){ thread(); }){
    // override required params to be sure
    connect_options_.set_automatic_reconnect(true);
    client_ = std::make_unique<mqtt::async_client>(mqtt_endpoint_, "", MAX_BUFFERED_MSGS, PERSIST_DIR); // todo check clientid & persisted messages - should be disabled???
    client_->set_callback(action_listener_);
  }

  mqtt_producer::~mqtt_producer() {
    exit_ = true;
    close();
    thread_.join();
    client_.reset(nullptr);
  }

  void mqtt_producer::register_metrics(kspp::processor *parent){
    parent->add_metric(&_connection_errors);
    parent->add_metric(&_msg_cnt);
    parent->add_metric(&_msg_bytes);
  }

  void mqtt_producer::close(){
    if (closed_)
      return;
    closed_ = true;
    if (client_ && client_->is_connected())
      client_->disconnect();
    LOG(INFO) << "mqtt_producer closed - written " << (int64_t)_msg_cnt.value() << " messages (" << (int64_t) _msg_bytes.value() << " bytes)";
    good_ = false;
    connected_ = false;
  }

  void mqtt_producer::poll(){
    while (!_done.empty()) {
      _done.pop_front();
    }
  }

  void mqtt_producer::on_connection_lost(){
    LOG(INFO) << "mqtt_producer::on_connection_lost, moving " << _pending_msg.size() << " messages back to send queue";
    ++_connection_errors;
    while (!_pending_msg.empty()){
      auto p = _pending_msg.back();
      _pending_msg.pop_back();
      _incomming_msg.push_front(p);
    }
  }

  // TODO do we have a guarantee that messages gets acked in order???
  void mqtt_producer::on_publish_success(){
    auto p = _pending_msg.pop_front_and_get();
    ++_msg_cnt;
    _msg_bytes += p->record()->key().size() + p->record()->value()->size();
    //LOG(INFO) << "on_publish_success lag " << kspp::milliseconds_since_epoch() - p->record()->event_time() << " ms";
    _done.push_back(p);
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
        if (!_pending_msg.empty())
          LOG(INFO) << "mqtt_producer::thread() - connection lost - moving " << _pending_msg.size() << " messages back to send queue";
        while (!_pending_msg.empty()) {
          auto p = _pending_msg.back();
          _pending_msg.pop_back();
          _incomming_msg.push_front(p);
        }
        continue;
      }

      while (!exit_ && !_incomming_msg.empty()) {
        auto msg = _incomming_msg.pop_front_and_get();
        if (msg->record()->value() == nullptr) {
          DLOG(INFO) << "skipping delete";
          _done.push_back(msg);
          continue;
        }
        _pending_msg.push_back(msg);
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