#include <chrono>
#include <memory>
#include <kspp/internal/queue.h>
#include <kspp/connect/postgres/postgres_connection.h>
#include <kspp/topology.h>
#include <kspp/connect/generic_producer.h>
#pragma once

namespace kspp {
  class postgres_producer : public generic_producer<kspp::generic_avro, kspp::generic_avro>
  {
  public:
    enum { MAX_ERROR_BEFORE_BAD=200 };
    postgres_producer(std::string table,
                      const kspp::connect::connection_params& cp,
                      std::string id_column,
                      std::string client_encoding,
                      size_t max_items_in_insert,
                      bool skip_delete=false);
    ~postgres_producer();

    void register_metrics(kspp::processor* parent) override;

    void close() override;

    bool good() const {
      return (_current_error_streak < MAX_ERROR_BEFORE_BAD);
    }

    bool eof() const override {
      return (_incomming_msg.empty() && _done.empty());
    }

    std::string topic() const override {
      return _table;
    }

    void stop();

    bool is_connected() const { return _connected; }

    void insert(std::shared_ptr<kevent<kspp::generic_avro, kspp::generic_avro>> p) override {
      _incomming_msg.push_back(p);
    }

    void poll() override;

    size_t queue_size() const override {
      auto sz0 =_incomming_msg.size();
      auto sz1 =_done.size();
      return sz0 + sz1;
    }

  private:
    bool initialize();
    bool check_table_exists();
    void _thread();

    bool _exit;
    bool _start_running;
    bool _good;
    bool _closed;
    bool _connected; // ??


    std::thread _bg;
    std::unique_ptr<kspp_postgres::connection> _connection;

    const std::string _table;
    const kspp::connect::connection_params cp_;

    const std::string _id_column;
    const std::string _client_encoding;

    event_queue<kspp::generic_avro, kspp::generic_avro> _incomming_msg;
    event_queue<kspp::generic_avro, kspp::generic_avro> _done;  // waiting to be deleted in poll();
    size_t _max_items_in_insert;

    bool _table_checked;
    bool _table_exists;
    bool _skip_delete2;

    size_t _current_error_streak;

    metric_counter _connection_errors;
    metric_counter _insert_errors;
    metric_counter _msg_cnt;
    metric_counter _msg_bytes;
    metric_summary _request_time;
  };
}

