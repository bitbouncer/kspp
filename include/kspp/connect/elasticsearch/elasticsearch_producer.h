#include <chrono>
#include <memory>
#include <kspp/kspp.h>
#include <kspp/impl/queue.h>
#include <kspp/utils/async.h>
#include <kspp/utils/http_client.h>
#include <kspp/avro/generic_avro.h>
#include <kspp/impl/event_queue.h>
#include <kspp/kevent.h>
#include <kspp/connect/generic_producer.h>
#include <kspp/connect/connection_params.h>
#include <kspp/metrics/metrics.h>
#pragma once

namespace kspp {
  class elasticsearch_producer : public generic_producer<kspp::generic_avro, kspp::generic_avro>
  {
  public:
    enum work_result_t { SUCCESS = 0, TIMEOUT = -1, HTTP_ERROR = -2, HTTP_BAD_REQUEST_ERROR = -4};

    elasticsearch_producer(const kspp::connect::connection_params& cp,
                           std::string id_column,
                           size_t http_batch_size);

    ~elasticsearch_producer();

    bool good() const {
      return _good;
    }

    void register_metrics(kspp::processor* parent) override;

    void close() override;

    bool eof() const override {
      return (_incomming_msg.empty() && _done.empty());
    }

    std::string topic() const override {
      return _cp.database_name;
    }

    void stop();

    bool is_connected() const { return _connected; }

    void insert(std::shared_ptr<kevent<kspp::generic_avro, kspp::generic_avro>> p) override{
      _incomming_msg.push_back(p);
    }

    void poll();

    size_t queue_size() const override {
      return _incomming_msg.size() + _done.size();
    }

  private:
    void connect_async();
    void check_table_exists_async();
    void _process_work(); // thread function;

    kspp::async::work<elasticsearch_producer::work_result_t>::async_function create_one_http_work(const kspp::generic_avro& key, const kspp::generic_avro* value);

    boost::asio::io_service _ios;
    std::shared_ptr<boost::asio::io_service::work> _work;

    std::thread _bg;
    kspp::http::client _http_handler;
    size_t _batch_size;
    std::chrono::milliseconds _http_timeout;

    const kspp::connect::connection_params _cp;
    const std::string _id_column;

    event_queue<kspp::generic_avro, kspp::generic_avro> _incomming_msg;
    event_queue<kspp::generic_avro, kspp::generic_avro> _done;

    bool _good;
    bool _connected;
    bool _closed;
    bool _table_checked;
    bool _table_exists;
    bool _table_create_pending;
    bool _insert_in_progress;

    std::thread _fg;
    metric_counter _timeout;
    metric_counter _http_2xx;
    metric_counter _http_3xx;
    metric_counter _http_404;
    metric_counter _http_4xx;
    metric_counter _http_5xx;
    metric_counter _msg_bytes;
    metric_summary _request_time;

    bool _skip_delete_of_non_active;
    std::set<std::string> _active_ids;
  };
}

