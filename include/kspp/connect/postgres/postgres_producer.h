#include <chrono>
#include <memory>
#include <kspp/impl/queue.h>
#include <kspp/connect/postgres/postgres_asio.h>
#include <kspp/topology.h>

#pragma once

namespace kspp {
  class postgres_producer
  {
  public:
    postgres_producer(std::string table,
                      std::string connect_string,
                      std::string id_column,
                      size_t max_items_in_fetch=1000);
    ~postgres_producer();

    void close();

    //std::unique_ptr<RdKafka::Message> consume();
    std::shared_ptr<PGresult> consume();

    inline bool eof() const {
      return _eof;
    }

    inline std::string table() const {
      return _table;
    }

    void stop();

    void subscribe();

    bool is_connected() const { return _connected; }
    bool is_query_running() const { return !_eof; }

  private:
    void connect_async();
    void check_table_exists_async();

    //void select_async();
    //void handle_fetch_cb(int ec, std::shared_ptr<PGresult> res);


    boost::asio::io_service _fg_ios;
    boost::asio::io_service _bg_ios;
    std::auto_ptr<boost::asio::io_service::work> _fg_work;
    std::auto_ptr<boost::asio::io_service::work> _bg_work;
    std::thread _fg;
    std::thread _bg;
    std::shared_ptr<postgres_asio::connection> _connection;

    //std::shared_ptr<connect_config> _config;
    const std::string _table;
    const std::string _connect_string;
    const std::string _id_column;

    //kspp::queue<std::shared_ptr<PGresult>> _queue;
    size_t _max_items_in_fetch;
    uint64_t _msg_cnt;
    uint64_t _msg_bytes;
    bool _good;
    bool _connected;
    bool _eof;
    bool _closed;
    bool _table_checked;
    bool _table_exists;
  };
}

