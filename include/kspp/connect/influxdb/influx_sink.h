#include <chrono>
#include <kspp/kspp.h>
#include <kspp/utils/async.h>
#include <kspp/utils/http_client.h>
#include <kspp/connect/connection_params.h>
#pragma once

namespace kspp {
  class influx_sink :
      public kspp::topic_sink<void, std::string>
  {
  public:
    influx_sink(std::shared_ptr<cluster_config> config,
                const kspp::connect::connection_params& cp,
                int32_t http_batch_size,
                std::chrono::milliseconds http_timeout);
    ~influx_sink() override;
    std::string log_name() const override;
    bool eof() const override;
    size_t process(int64_t tick) override;
    void close() override;
    void flush() override;
  private:
    void _thread();

    bool _exit;
    bool _start_running;
    bool _good;
    bool _closed;

    boost::asio::io_service _ios;
    std::unique_ptr<boost::asio::io_service::work> _work;
    std::thread _asio_thread; // internal to http client
    std::thread _bg; // performs the send loop
    event_queue<void, std::string> _pending_for_delete;
    const kspp::connect::connection_params _cp;
    kspp::http::client _http_handler;
    size_t _batch_size;
    int64_t _next_time_to_send;
    int64_t _next_time_to_poll;
    std::chrono::milliseconds _http_timeout;
    kspp::metric_streaming_lag _lag;
    kspp::metric_counter _http_requests;
    kspp::metric_counter _http_timeouts;
    kspp::metric_counter _http_error;
    kspp::metric_counter _http_ok;
    kspp::metric_counter _http_bytes;
  };
} // namespace