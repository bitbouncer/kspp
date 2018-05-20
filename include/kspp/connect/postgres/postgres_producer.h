#include <chrono>
#include <memory>
#include <kspp/impl/queue.h>
#include <kspp/connect/postgres/postgres_connection.h>
#include <kspp/topology.h>
#include <kspp/connect/generic_producer.h>
#pragma once

namespace kspp {
  class postgres_producer : public generic_producer<void, kspp::GenericAvro>
  {
  public:
    postgres_producer(std::string table,
                      const kspp::connect::connection_params& cp,
                      std::string id_column,
                      std::string client_encoding,
                      size_t max_items_in_fetch=1000);
    ~postgres_producer();

    void close() override;

    bool eof() const override {
      return (_incomming_msg.empty() && _pending_for_delete.empty());
    }

    std::string topic() const override {
      return _table;
    }

    void stop();

    bool is_connected() const { return _connected; }

    void insert(std::shared_ptr<kevent<void, kspp::GenericAvro>> p) override {
      _incomming_msg.push_back(p);
    }

    void poll() override;

  private:
    bool initialize();
    bool check_table_exists();

    void write_some_async();
    void _thread();

    bool _exit;
    bool _start_running;
    bool _good;
    bool _closed;
    bool _connected; // ??


    std::thread _bg;
    std::shared_ptr<kspp_postgres::connection> _connection;

    const std::string _table;
    const kspp::connect::connection_params cp_;

    const std::string _id_column;
    const std::string _client_encoding;

    event_queue<void, kspp::GenericAvro> _incomming_msg;
    event_queue<void, kspp::GenericAvro> _pending_for_delete;
    size_t _max_items_in_fetch;
    uint64_t _msg_cnt;
    uint64_t _msg_bytes;

    bool _table_checked;
    bool _table_exists;
  };
}

