#include <chrono>
#include <memory>
#include <kspp/impl/queue.h>
#include <kspp/connect/postgres/postgres_asio.h>
#include <kspp/topology.h>

#pragma once

namespace kspp {
  class postgres_consumer
  {
  public:
    postgres_consumer(int32_t partition,
                      std::string table,
                      std::string consumer_group,
                      std::string connect_string,
                      std::string id_column,
                      std::string ts_column,
                      size_t max_items_in_fetch=1000);
    ~postgres_consumer();

    void close();

    //std::unique_ptr<RdKafka::Message> consume();
    std::shared_ptr<PGresult> consume();

    inline bool eof() const {
      return _eof;
    }

    inline std::string table() const {
      return _table;
    }

    inline int32_t partition() const {
      return _partition;
    }

    void start(int64_t offset);

    void stop();

    int32_t commit(int64_t offset, bool flush = false);

    inline int64_t commited() const {
      return _can_be_committed;
    }

    int update_eof();

    void subscribe();

    bool is_connected() const { return _connected; }
    bool is_query_running() const { return !_eof; }


  private:
    void connect_async();
    void select_async();
    void handle_fetch_cb(int ec, std::shared_ptr<PGresult> res);


    boost::asio::io_service _fg_ios;
    boost::asio::io_service _bg_ios;
    std::auto_ptr<boost::asio::io_service::work> _fg_work;
    std::auto_ptr<boost::asio::io_service::work> _bg_work;
    std::thread _fg;
    std::thread _bg;
    std::shared_ptr<postgres_asio::connection> _connection;

    //std::shared_ptr<connect_config> _config;
    const std::string _table;
    const int32_t _partition;
    const std::string _consumer_group;
    const std::string _connect_string;
    const std::string _id_column;
    const std::string _ts_column;


    kspp::queue<std::shared_ptr<PGresult>> _queue;
    size_t _max_items_in_fetch;
    int64_t _can_be_committed;
    int64_t _last_committed;
    size_t _max_pending_commits;
    uint64_t _msg_cnt;
    uint64_t _msg_bytes;
    bool _good;
    bool _connected;
    bool _eof;
    bool _closed;
  };
}

