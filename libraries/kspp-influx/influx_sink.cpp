#include <kspp-influx/influx_sink.h>

using namespace std::chrono_literals;

namespace kspp {
  influx_sink::influx_sink(std::shared_ptr<cluster_config> config,
                           const kspp::connect::connection_params &cp,
                           int32_t http_batch_size,
                           std::chrono::milliseconds http_timeout)
      : kspp::topic_sink<void, std::string>()
        , work_(std::make_unique<boost::asio::io_service::work>(ios_))
        , cp_(cp)
        , http_handler_(ios_, http_batch_size)
        , batch_size_(http_batch_size)
        , http_timeout_(http_timeout)
        , http_requests_("http_request", "msg")
        , http_timeouts_("http_timeout", "msg")
        , http_error_("http_error", "msg")
        , http_ok_("http_ok", "msg")
        , http_bytes_("http_bytes", "bytes")
        , asio_thread_([this] { ios_.run(); }), bg_([this] { _thread(); }) {
    this->add_metric(&lag_);
    this->add_metric(&http_bytes_);
    this->add_metric(&http_requests_);
    this->add_metric(&http_timeouts_);
    this->add_metric(&http_error_);
    this->add_metric(&http_ok_);
    this->add_metrics_label(KSPP_PROCESSOR_TYPE_TAG, "influx_sink");
    curl_global_init(CURL_GLOBAL_NOTHING); /* minimal */
    http_handler_.set_user_agent(
        "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36");
    start_running_ = true;
  }

  influx_sink::~influx_sink() {
    if (!closed_)
      close();
    exit_ = true;
    http_handler_.close();
    work_.reset(nullptr);
    asio_thread_.join();
    bg_.join();
  }

  std::string influx_sink::log_name() const {
    return "influx_sink";
  }

  bool influx_sink::eof() const {
    return (!batch_in_progress_ && (this->queue_.size() == 0) && (pending_for_delete_.size() == 0));
  }

  size_t influx_sink::process(int64_t tick) {
    size_t sz = 0;
    while (!pending_for_delete_.empty()) {
      ++sz;
      pending_for_delete_.pop_front();
    }
    return sz;
  }

  void influx_sink::close() {
    if (!closed_) {
      closed_ = true;
    }
    //TODO??
  }

  void influx_sink::_thread() {

    while (!exit_) {
      if (closed_)
        break;

      // connected
      if (!start_running_) {
        std::this_thread::sleep_for(1s);
        continue;
      }

      if (this->queue_.empty()) {
        std::this_thread::sleep_for(100ms);
        continue;
      }

      batch_in_progress_ = true;
      //size_t items_to_copy = std::min<size_t>(this->_queue.size(), _batch_size);
      std::string url = cp_.url + "/write?db=" + cp_.database_name;
      std::shared_ptr<kspp::http::request> request(new kspp::http::request(kspp::http::POST, url, {}, http_timeout_));

      size_t msg_in_batch = 0;
      size_t bytes_in_batch = 0;
      event_queue<void, std::string> in_batch;
      while (!this->queue_.empty() && msg_in_batch < batch_size_) {
        auto msg = this->queue_.pop_front_and_get(); // this will loose messages at retry... TODO
        //make sure no nulls gets to us
        if (msg->record()->value()) {
          request->append(*msg->record()->value());
          request->append("\n");
        }
        ++msg_in_batch;
        in_batch.push_back(msg);
      }

      bytes_in_batch += request->tx_content_length();

      //DLOG(INFO) << request->uri();
      //DLOG(INFO) << request->tx_content();

      //auto ts0 = kspp::milliseconds_since_epoch();
      // retry sent till we have to exit
      while (!exit_) {
        auto res = http_handler_.perform(request);
        ++http_requests_;
        // NOT OK?
        if (!res->ok()) {
          if (!res->transport_result()) {
            LOG(ERROR) << "HTTP timeout";
            std::this_thread::sleep_for(10s);
            ++http_timeouts_;
            continue;
          }
          LOG(ERROR) << "HTTP error (skipping) : " << res->rx_content();
          ++http_error_;
          // we have a partial write that is evil - if we have a pare error int kafka the we will stoip forever here if we don't skip that.
          // for now skip the error and contine as if it worked
          //std::this_thread::sleep_for(10s);
          //continue;
        }
        LOG(INFO) << "wrote " << bytes_in_batch << " bytes , in_batch:" << in_batch.size();
        ++http_ok_;
        // OK...
        //auto ts1 = kspp::milliseconds_since_epoch();
        while (!in_batch.empty())
          pending_for_delete_.push_back(in_batch.pop_front_and_get());
        //_msg_cnt += msg_in_batch;
        http_bytes_ += bytes_in_batch;
        break;
      }
      batch_in_progress_ = false;
    } // while (!exit)
    DLOG(INFO) << "exiting thread";
  }

  void influx_sink::flush() {
    LOG(INFO) << "flushing input";
    while (!eof()) {
      process(kspp::milliseconds_since_epoch());
      poll(0);
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    LOG(INFO) << "flushing output buffer";
    while (true) {
      int ec = 0; // TODO fixme
      //auto ec = _impl.flush(1000);
      if (ec == 0)
        break;
    }
    LOG(INFO) << "flushing done";
  }
} // namespace

