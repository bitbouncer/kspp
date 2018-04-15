#include "influx_sink.h"

influx_sink::influx_sink(kspp::topology &,
                         std::string base_url,
                         int32_t http_batch_size,
                         std::chrono::milliseconds http_timeout)
    : kspp::topic_sink<void, std::string>(),
      _work(std::make_unique<boost::asio::io_service::work>(_ios))
    , _thread([this] { _ios.run(); })
    , _base_url(base_url)
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
}

influx_sink::~influx_sink() {
  _http_handler.close();
  _work = nullptr;
  _thread.join();
  //_source->remove_sink()
}

std::string influx_sink::log_name() const {
  return "influx_sink";
}

bool influx_sink::eof() const {
  return (this->_queue.size() == 0);
}

size_t influx_sink::process(int64_t tick) {
  size_t sz = this->_queue.size();
  if (sz >= _batch_size || tick > _next_time_to_send) {
    _next_time_to_send = kspp::milliseconds_since_epoch() + 100;
    if (sz > 0) {
      send();
      _next_time_to_send = kspp::milliseconds_since_epoch() + 100; // give some time for more data to come in - it's easy to get into the state where we send one message at a time...
    }
    return sz;
  }
  return 0;
}

void influx_sink::close() {
  //todo ?? _http_handler.close();
  // do nothing (maybe cancel calls in progress)
}

void influx_sink::run_work(std::deque<kspp::async::work<influx_sink::work_result_t>::async_function> &work, size_t batch_size) {
  //TODO should we have a base url block list that points to dead servers that goes directly to dead letter queue??
  while (work.size()) {
    kspp::async::work<influx_sink::work_result_t> batch(kspp::async::PARALLEL, kspp::async::ALL);
    size_t nr_of_items_in_batch = std::min<size_t>(work.size(), batch_size);
    for (int i = 0; i != nr_of_items_in_batch; ++i) {
      batch.push_back(work[0]);
      work.pop_front();
    }
    batch();
    for (int i = 0; i != nr_of_items_in_batch; ++i) {
      ++_http_requests;
      auto p = batch.get_result(i);
      switch (batch.get_result(i)) {
        case influx_sink::SUCCESS:
          ++_http_ok;
          break;
        case influx_sink::TIMEOUT:
          ++_http_timeouts;
          //TODO send the bad requests toa dead letter queue
          break;
        case influx_sink::HTTP_ERROR:
          ++_http_error;
          //TODO send the bad requests toa dead letter queue
          break;
      }
    }
  }
}

kspp::async::work<influx_sink::work_result_t>::async_function
influx_sink::create_work(std::shared_ptr<kspp::kevent<void, std::string>> ev) {
  kspp::async::work<work_result_t>::async_function f = [this, ev](std::function<void(work_result_t)> cb) {
    auto record = ev->record();
    auto request = std::make_shared<kspp::http::request>(kspp::http::POST, _base_url, {}, _http_timeout);
    _http_handler.perform_async(request, [this, cb](std::shared_ptr<kspp::http::request> h) {
      if (!h->ok()) {
        if (!h->transport_result()) {
          cb(TIMEOUT);
          return;
        } else {
          cb(HTTP_ERROR);
          return;
        }
      } else {
        DLOG_EVERY_N(INFO, 1000) << "influx_sink, http get: " << h->uri() << " got " << h->rx_content_length()
                                 << " bytes, time=" << h->milliseconds() << " ms";
        cb(SUCCESS);
      }
    }); // handler
  }; // work
  return f;
}

void influx_sink::send() {
  if (this->_queue.size() > 0) {
    int64_t last_event_time=0;
    std::deque<std::shared_ptr<kspp::kevent<void, std::string>>> items;
    size_t items_to_copy = std::min<size_t>(this->_queue.size(), _batch_size);

    std::deque<kspp::async::work<work_result_t>::async_function> work;
    for (size_t i = 0; i != items_to_copy; ++i) {
      auto p = this->_queue.front();
      last_event_time = p->event_time();
      if (p->record()->value()) {
        auto f = create_work(p);
        if (f)
          work.push_back(f);
      }
      this->_queue.pop_front();
    }

    if (work.size()) {
      size_t ws = work.size();
      auto start = kspp::milliseconds_since_epoch();
      run_work(work, _batch_size);
      auto end = kspp::milliseconds_since_epoch();
      _lag.add_event_time(end, last_event_time);
      LOG(INFO) << "influx_sink, done, worksize: " << ws << ", batch_size: " << _batch_size << ", duration: "
                << end - start << " ms";
    }
    return;
  }
}


