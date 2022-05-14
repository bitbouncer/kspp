#include <chrono>
#include <kspp/kspp.h>
#include <kspp/utils/async.h>
#include <kspp/utils/http_client.h>
#include <kspp/connect/connection_params.h>

#pragma once

namespace kspp {
  class influx_sink :
      public kspp::topic_sink<void, std::string> {
  public:
    influx_sink(std::shared_ptr<cluster_config> config,
                const kspp::connect::connection_params &cp,
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

    boost::asio::io_service ios_;
    std::unique_ptr<boost::asio::io_service::work> work_;
    bool exit_=false;
    bool start_running_=false;
    bool closed_=false;
    std::atomic<bool> batch_in_progress_ = false;
    event_queue<void, std::string> pending_for_delete_;
    const kspp::connect::connection_params cp_;
    kspp::http::client http_handler_;
    const size_t batch_size_;
    const std::chrono::milliseconds http_timeout_;
    kspp::metric_streaming_lag lag_;
    kspp::metric_counter http_requests_;
    kspp::metric_counter http_timeouts_;
    kspp::metric_counter http_error_;
    kspp::metric_counter http_ok_;
    kspp::metric_counter http_bytes_;
    std::thread asio_thread_; // internal to http client
    std::thread bg_; // performs the send loop
  };
} // namespace