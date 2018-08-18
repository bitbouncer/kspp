#include <kspp/connect/influxdb/influx_sink.h>

using namespace std::chrono_literals;

namespace kspp {
  influx_sink::influx_sink(std::shared_ptr<cluster_config> config,
                           const kspp::connect::connection_params& cp,
                           int32_t http_batch_size,
                           std::chrono::milliseconds http_timeout)
      : kspp::topic_sink<void, std::string>()
      , _good(true)
      , _closed(false)
      , _start_running(false)
      , _exit(false)
      , _work(std::make_unique<boost::asio::io_service::work>(_ios))
      , _asio_thread([this] { _ios.run(); })
      , _bg([this] { _thread(); })
      , _cp(cp)
      , _http_handler(_ios, http_batch_size)
      , _batch_size(http_batch_size)
      , _next_time_to_send(kspp::milliseconds_since_epoch() + 100)
      , _next_time_to_poll(0)
      , _http_timeout(http_timeout)
      , _http_bytes("http_bytes", "bytes")
      , _http_requests("http_request", "msg")
      , _http_timeouts("http_timeout", "msg")
      , _http_error("http_error", "msg")
      , _http_ok("http_ok", "msg") {
    this->add_metric(&_lag);
    this->add_metric(&_http_bytes);
    this->add_metric(&_http_requests);
    this->add_metric(&_http_timeouts);
    this->add_metric(&_http_error);
    this->add_metric(&_http_ok);
    this->add_metrics_tag(KSPP_PROCESSOR_TYPE_TAG, "influx_sink");

    curl_global_init(CURL_GLOBAL_NOTHING); /* minimal */
    _http_handler.set_user_agent(
        "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36");

    _start_running = true;
  }

  influx_sink::~influx_sink() {
    if (!_closed)
      close();
    _exit = true;
    _http_handler.close();
    _work = nullptr;
    _asio_thread.join();
    _bg.join();
  }

  std::string influx_sink::log_name() const {
    return "influx_sink";
  }

  bool influx_sink::eof() const {
    return ((this->_queue.size() == 0) && (_pending_for_delete.size()==0));
  }

  size_t influx_sink::process(int64_t tick) {
    size_t sz=0;
    while (!_pending_for_delete.empty()) {
      ++sz;
      _pending_for_delete.pop_front();
    }
    return sz;
  }

  void influx_sink::close() {
    if (!_closed){
      _closed = true;
    }
    //TODO??
  }

  void influx_sink::_thread() {

    while (!_exit) {
      if (_closed)
        break;

      // connected
      if (!_start_running) {
        std::this_thread::sleep_for(1s);
        continue;
      }

      if (this->_queue.empty()) {
        std::this_thread::sleep_for(100ms);
        continue;
      }

      size_t items_to_copy = std::min<size_t>(this->_queue.size(), _batch_size);
      std::string url = _cp.url + "/write?db=" + _cp.database;
      std::shared_ptr<kspp::http::request> request(new kspp::http::request(kspp::http::POST, url, {}, _http_timeout));

      size_t msg_in_batch = 0;
      size_t bytes_in_batch = 0;
      event_queue<void, std::string> in_batch;
      while (!this->_queue.empty() && msg_in_batch < _batch_size) {
        auto msg = this->_queue.pop_and_get(); // this will loose messages at retry... TODO
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

      auto ts0 = kspp::milliseconds_since_epoch();
      // retry sent till we have to exit
      while(!_exit) {
        auto res = _http_handler.perform(request, true);

        // NOT OK?
        if (!res->ok()) {
          if (!res->transport_result()) {
            LOG(ERROR) << "HTTP timeout";
            std::this_thread::sleep_for(10s);
            continue;
          }
          LOG(ERROR) << "HTTP error (skipping) : " << res->rx_content();
          // we have a partial write that is evil - if we have a pare error int kafka the we will stoip forever here if we don't skip that.
          // for now skip the error and contine as if it worked
          //std::this_thread::sleep_for(10s);
          //continue;
          }

          // OK...
          auto ts1 = kspp::milliseconds_since_epoch();
          LOG(INFO) << "HTTP call nr of msg: " << msg_in_batch  << ", (" << ts1 - ts0 << ") ms";
          //DLOG_EVERY_N(INFO, 1000) << "influx_sink, http post: " << h->uri() << " got " << h->rx_content_length() << " bytes, time=" << h->milliseconds() << " ms";

          while (!in_batch.empty()) {
            _pending_for_delete.push_back(in_batch.pop_and_get());
          }

          //_msg_cnt += msg_in_batch;
          _http_bytes += bytes_in_batch;
          break;
        }
    } // while (!exit)
    DLOG(INFO) << "exiting thread";
  }

  void influx_sink::flush() {
    while (!eof()) {
      process(kspp::milliseconds_since_epoch());
      poll(0);
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    while (true) {
      int ec = 0; // TODO fixme
      //auto ec = _impl.flush(1000);
      if (ec == 0)
        break;
    }
  }
} // namespace

