#include <chrono>
#include <kspp/kspp.h>
#include <kspp/utils/async.h>
#include <kspp/utils/http_client.h>
#pragma once

class influx_sink : public kspp::topic_sink<void, std::string>
{
public:
  enum work_result_t { SUCCESS = 0, TIMEOUT = -1, HTTP_ERROR = -2 };

  influx_sink(kspp::topology&,
            std::string base_url,
            int32_t http_batch_size,
            std::chrono::milliseconds http_timeout);
  ~influx_sink() override;
  std::string simple_name() const override;
  bool eof() const override;
  bool process_one(int64_t tick) override;
  void close() override;
  void flush() override;
private:
  kspp::async::work<work_result_t>::async_function create_work(std::shared_ptr<kspp::kevent<void, std::string>> record);
  void send();

private:
  void run_work(std::deque<kspp::async::work<influx_sink::work_result_t>::async_function> &work, size_t batch_size);

  boost::asio::io_service                        _ios;
  std::unique_ptr<boost::asio::io_service::work> _work;
  std::thread                                    _thread;
  std::string                                    _base_url;
  kspp::http::client                             _http_handler;
  size_t                                         _batch_size;
  int64_t                                        _next_time_to_send;
  int64_t                                        _next_time_to_poll;
  std::chrono::milliseconds                      _http_timeout;
  kspp::metric_lag                               _lag;
  kspp::metric_counter                           _http_requests;
  kspp::metric_counter                           _http_timeouts;
  kspp::metric_counter                           _http_error;
  kspp::metric_counter                           _http_ok;
  kspp::metric_counter                           _http_bytes;
};
