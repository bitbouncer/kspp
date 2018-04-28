#include <kspp/connect/postgres/postgres_producer.h>

#include <kspp/connect/postgres/postgres_consumer.h>
#include <kspp/kspp.h>
#include <chrono>
#include <memory>
#include <glog/logging.h>
#include <boost/bind.hpp>

namespace kspp {
  postgres_producer::postgres_producer(std::string table, std::string connect_string, std::string id_column, size_t max_items_in_fetch)
      : _fg_work(new boost::asio::io_service::work(_fg_ios))
      , _bg_work(new boost::asio::io_service::work(_bg_ios))
      , _fg(boost::bind(&boost::asio::io_service::run, &_fg_ios))
      , _bg(boost::bind(&boost::asio::io_service::run, &_bg_ios))
      , _connection(std::make_shared<postgres_asio::connection>(_fg_ios, _bg_ios))
      , _table(table)
      , _connect_string(connect_string)
      , _id_column(id_column)
      , _max_items_in_fetch(max_items_in_fetch)
      , _msg_cnt(0)
      , _msg_bytes(0)
      , _good(true)
      , _closed(false)
      , _eof(false)
      , _connected(false)
      , _table_checked(false)
      , _table_exists(false) {
    connect_async();
  }

  postgres_producer::~postgres_producer(){
    if (!_closed)
      close();

    _fg_work.reset();
    _bg_work.reset();
    //bg_ios.stop();
    //fg_ios.stop();
    _bg.join();
    _fg.join();

    _connection->close();
    _connection = nullptr;
  }

  void postgres_producer::close(){
    if (_closed)
      return;
    _closed = true;

    if (_connection) {
      _connection->close();
      LOG(INFO) << "postgres_producer table:" << _table << ", closed - consumed " << _msg_cnt << " messages (" << _msg_bytes << " bytes)";
    }

    _connected = false;
  }

  /*
   * std::shared_ptr<PGresult> postgres_producer::consume(){
    if (_queue.empty())
      return nullptr;
    auto p = _queue.pop_and_get();
    return p;
  }
   */

  void postgres_producer::stop(){

  }


  /*int postgres_producer::update_eof() {

  }
   */

  void postgres_producer::connect_async() {
    DLOG(INFO) << "connecting : connect_string: " <<  _connect_string;
    _connection->connect(_connect_string, [this](int ec) {
      if (!ec) {
        DLOG(INFO) << "connected";
        bool r1 = _connection->set_client_encoding("UTF8");
        _good = true;
        _connected = true;
        _eof = true;
        check_table_exists_async();
      } else {
        LOG(ERROR) << "connect failed";
        _good = false;
        _connected = false;
        _eof = true;
      }
    });
  }

  /*
  SELECT EXISTS (
   SELECT 1
   FROM   pg_tables
   WHERE  schemaname = 'schema_name'
   AND    tablename = 'table_name'
   );
   */

  void postgres_producer::check_table_exists_async() {
    std::string statement = "SELECT 1 FROM pg_tables WHERE tablename = '" + _table + "'";
      DLOG(INFO) << "exec(" + statement + ")";
      _connection->exec(statement,
                        [this, statement](int ec, std::shared_ptr<PGresult> res) {

                          if (ec) {
                            LOG(FATAL) << statement  << " failed ec:" << ec << " last_error: " << _connection->last_error();
                            _table_checked = true;
                            return;
                          }

                          int tuples_in_batch = PQntuples(res.get());

                          if(tuples_in_batch>0) {
                            LOG(INFO) << _table << " exists";
                            _table_exists = true;
                          } else {
                            LOG(INFO) << _table << " not existing - will be created later";
                          }

                          _table_checked = true;
    });
  }

/*
  void postgres_producer::handle_fetch_cb(int ec, std::shared_ptr<PGresult> result) {
    if (ec)
      return;
    int tuples_in_batch = PQntuples(result.get());
    _msg_cnt += tuples_in_batch;

    // should this be here?? it costs something
    size_t nFields = PQnfields(result.get());
    for (int i = 0; i < tuples_in_batch; i++)
      for (int j = 0; j < nFields; j++)
        _msg_bytes += PQgetlength(result.get(), i, j);

    if (tuples_in_batch == 0) {
      DLOG(INFO) << "query done, got total: " << _msg_cnt; // tbd remove this
      _connection->exec("CLOSE MYCURSOR; COMMIT", [](int ec, std::shared_ptr<PGresult> res) {});
      _eof = true;
      return;
    } else {
      DLOG(INFO) << "query batch done, got total: " << _msg_cnt; // tbd remove this
      _queue.push_back(result);
      _connection->exec("FETCH " + std::to_string(_max_items_in_fetch) + " IN MYCURSOR",
                        [this](int ec, std::shared_ptr<PGresult> res) {
                          this->handle_fetch_cb(ec, std::move(res));
                        });
    }
  }
  */

//  void postgres_producer::select_async()
//  {
//    // connected
//    if (!_connected)
//      return;
//
//    // already runnning
//    if (!_eof)
//      return;
//
//    DLOG(INFO) << "exec(BEGIN)";
//    _eof = false;
//    _connection->exec("BEGIN", [this](int ec, std::shared_ptr<PGresult> res) {
//      if (ec) {
//        LOG(FATAL) << "BEGIN failed ec:" << ec << " last_error: " << _connection->last_error();
//        return;
//      }
//      std::string fields = "*";
//
//      std::string order_by = "";
//      if (_ts_column.size())
//        order_by = _ts_column + " ASC, " + _id_column + " ASC";
//      else
//        order_by = _id_column + " ASC";
//
//      std::string where_clause;
//
//      if (_ts_column.size())
//        where_clause = _ts_column;
//      else
//        where_clause = _id_column;
//
//      std::string statement = "DECLARE MYCURSOR CURSOR FOR SELECT " + fields + " FROM "+ _table + " ORDER BY " + order_by;
//      DLOG(INFO) << "exec(" + statement + ")";
//      _connection->exec(statement,
//                        [this](int ec, std::shared_ptr<PGresult> res) {
//                          if (ec) {
//                            LOG(FATAL) << "DECLARE MYCURSOR... failed ec:" << ec << " last_error: " << _connection->last_error();
//                            return;
//                          }
//                          _connection->exec("FETCH " + std::to_string(_max_items_in_fetch) +" IN MYCURSOR",
//                                            [this](int ec, std::shared_ptr<PGresult> res) {
//                                              try {
//                                                /*
//                                                boost::shared_ptr<avro::ValidSchema> schema(
//                                                    valid_schema_for_table_row(_table  + ".value", res));
//                                                boost::shared_ptr<avro::ValidSchema> key_schema(
//                                                    valid_schema_for_table_key(_table + ".key", {"id"}, res));
//
//                                                std::cerr << "key schema" << std::endl;
//                                                key_schema->toJson(std::cerr);
//                                                std::cerr << std::endl;
//
//                                                std::cerr << "value schema" << std::endl;
//                                                std::cerr << std::endl;
//                                                schema->toJson(std::cerr);
//                                                std::cerr << std::endl;
//  */
//                                                handle_fetch_cb(ec, std::move(res));
//                                              }
//                                              catch (std::exception &e) {
//                                                LOG(FATAL) << "exception: " << e.what();
//                                              };
//                                              /*
//                                              int nFields = PQnfields(res.get());
//                                              for (int i = 0; i < nFields; i++)
//                                                  printf("%-15s", PQfname(res.get(), i));
//                                              */
//                                            });
//                        });
//    });
//  }

}

