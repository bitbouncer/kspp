#include <chrono>
#include <memory>
#include <kspp/impl/queue.h>
#include <kspp/utils/async.h>
#include <kspp/utils/http_client.h>
#include <kspp/avro/avro_generic.h>
#include <kspp/impl/event_queue.h>
#include <kspp/kevent.h>
#include <kspp/connect/generic_producer.h>

#pragma once

namespace kspp {
  class elasticsearch_producer : public generic_producer<void, kspp::GenericAvro>
  {
  public:
    enum work_result_t { SUCCESS = 0, TIMEOUT = -1, HTTP_ERROR = -2, PARSE_ERROR = -3 };

    elasticsearch_producer(std::string index_name,
                      std::string base_url,
                      std::string user,
                      std::string password,
                      std::string id_column,
                      size_t http_batch_size);

    ~elasticsearch_producer();

    void close() override;


    bool eof() const override {
      return (_incomming_msg.empty() && _pending_for_delete.empty());
    }

    std::string topic() const override {
      return _index_name;
    }

    void stop();

    //void subscribe();

    bool is_connected() const { return _connected; }
    //bool is_query_running() const { return !_eof; }

    void insert(std::shared_ptr<kevent<void, kspp::GenericAvro>> p) override{
      _incomming_msg.push_back(p);
    }

    void poll();

  private:
    void connect_async();
    void check_table_exists_async();
    void write_some_async();

    void _process_work(); // thread function;

    kspp::async::work<elasticsearch_producer::work_result_t>::async_function create_one_http_work(const kspp::GenericAvro& doc);


      //void select_async();
    //void handle_fetch_cb(int ec, std::shared_ptr<PGresult> res);

    boost::asio::io_service _ios;
    std::shared_ptr<boost::asio::io_service::work> _work;

    std::thread _bg;
    kspp::http::client _http_handler;
    size_t _batch_size;
    std::chrono::milliseconds _http_timeout;

    const std::string _index_name;
    const std::string _base_url;
    const std::string _user;
    const std::string _password;
    const std::string _id_column;

    event_queue<void, kspp::GenericAvro> _incomming_msg;
    event_queue<void, kspp::GenericAvro> _pending_for_delete;
    uint64_t _msg_cnt;
    uint64_t _msg_bytes;
    bool _good;
    bool _connected;
    bool _closed;
    bool _table_checked;
    bool _table_exists;
    bool _table_create_pending;
    bool _insert_in_progress;

    std::thread _fg;
  };
}

