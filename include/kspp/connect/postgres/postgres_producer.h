#include <chrono>
#include <memory>
#include <kspp/impl/queue.h>
#include <kspp/connect/postgres/postgres_asio.h>
#include <kspp/topology.h>
#include <kspp/connect/generic_producer.h>
#pragma once

namespace kspp {
  class postgres_producer : public generic_producer<void, kspp::GenericAvro>
  {
  public:
    postgres_producer(std::string table,
                      std::string connect_string,
                      std::string id_column,
                      std::string client_encoding,
                      size_t max_items_in_fetch=1000);
    ~postgres_producer();

    void close() override;



    //std::unique_ptr<RdKafka::Message> consume();
    //std::shared_ptr<PGresult> consume();

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
    void connect_async();
    void check_table_exists_async();
    void write_some_async();

    boost::asio::io_service _fg_ios;
    boost::asio::io_service _bg_ios;
    std::auto_ptr<boost::asio::io_service::work> _fg_work;
    std::auto_ptr<boost::asio::io_service::work> _bg_work;
    std::thread _fg;
    std::thread _bg;
    std::shared_ptr<postgres_asio::connection> _connection;

    const std::string _table;
    const std::string _connect_string;
    const std::string _id_column;
    const std::string _client_encoding;

    event_queue<void, kspp::GenericAvro> _incomming_msg;
    event_queue<void, kspp::GenericAvro> _pending_for_delete;
    size_t _max_items_in_fetch;
    uint64_t _msg_cnt;
    uint64_t _msg_bytes;
    bool _good;
    bool _connected;
    bool _closed;
    bool _table_checked;
    bool _table_exists;
    bool _table_create_pending;
    bool _insert_in_progress;
  };
}

