#include <kspp/connect/postgres/postgres_producer.h>
#include <chrono>
#include <memory>
#include <glog/logging.h>
#include <boost/bind.hpp>
#include <kspp/kspp.h>
#include <kspp/connect/postgres/postgres_avro_utils.h>

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
      , _connected(false)
      , _table_checked(false)
      , _table_exists(false)
      , _table_create_pending(false)
      , _insert_in_progress(false){
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
      LOG(INFO) << "postgres_producer table:" << _table << ", closed - producer " << _msg_cnt << " messages (" << _msg_bytes << " bytes)";
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
        check_table_exists_async();
      } else {
        LOG(ERROR) << "connect failed";
        _good = false;
        _connected = false;
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

  void postgres_producer::write_some_async() {
    if (_incomming_msg.empty())
      return;

    if (_insert_in_progress)
      return;

    if (!_table_exists){
      if (_table_create_pending)
        return;
      _table_create_pending = true;
      auto msg = _incomming_msg.front();

      //TODO verify that the data actually has the _id_column(s)

      std::string statement = avro2sql_create_table_statement(_table, _id_column, *msg->record()->value()->valid_schema());
      DLOG(INFO) << "exec(" + statement + ")";
      _connection->exec(statement,
                        [this, statement](int ec, std::shared_ptr<PGresult> res) {

                          if (ec) {
                            LOG(FATAL) << statement  << " failed ec:" << ec << " last_error: " << _connection->last_error();
                            _table_checked = true;
                            _table_create_pending = false;
                            return;
                          }
                          _table_exists = true;
                          _table_create_pending = false;
                        });
      return; // we must exit since we must wait for completion of above
    }

    _insert_in_progress=true;

    auto msg = _incomming_msg.front();
    std::string statement = avro2sql_build_insert_1(_table, *msg->record()->value()->valid_schema());
    std::string upsert_part = avro2sql_build_upsert_2(_table, _id_column, *msg->record()->value()->valid_schema());

    size_t msg_in_batch = 0 ;
    size_t bytes_in_batch=0;
    std::set<std::string> unique_keys_in_batch;
    while(!_incomming_msg.empty() && msg_in_batch<1000) {
      auto msg = _incomming_msg.front();

      // we cannot have the id columns of this update more than once
      // postgres::exec failed ERROR:  ON CONFLICT DO UPDATE command cannot affect row a second time
      auto key_string = avro2sql_key_values(*msg->record()->value()->valid_schema(), _id_column, *msg->record()->value()->generic_datum());
      auto res = unique_keys_in_batch.insert(key_string);
      if (res.second==false) {
        DLOG(INFO) << "breaking up upsert due to 'ON CONFLICT DO UPDATE command cannot affect row a second time...' batch size: " << msg_in_batch;
        break;
      }

      if (msg_in_batch>0)
        statement += ", \n";
      statement += avro2sql_values(*msg->record()->value()->valid_schema(), *msg->record()->value()->generic_datum());;
      ++msg_in_batch;
      _pending_for_delete.push_back(msg);
      _incomming_msg.pop_front();
    }
    statement += "\n";
    statement += upsert_part;
    bytes_in_batch +=  statement.size();
    //std::cerr << statement << std::endl;

    _connection->exec(statement,
                      [this, statement, msg_in_batch, bytes_in_batch](int ec, std::shared_ptr<PGresult> res) {

                        if (ec) {
                          LOG(FATAL) << statement  << " failed ec:" << ec << " last_error: " << _connection->last_error();
                          _insert_in_progress=false;
                          return;
                        }

                        _msg_cnt += msg_in_batch;
                        _msg_bytes += bytes_in_batch;

                        // frees the commit markers
                        while(!_pending_for_delete.empty()) {
                          _pending_for_delete.pop_front();
                        }

                        _insert_in_progress=false;
                        // TODO we should proably call ourselves were but since this is called from boost thread
                        // TODO we nned to use a post in the main thread to trigger write_some_async from boost thread
                        // this is handled through poll() now...
                      });
  }


  void postgres_producer::poll() {
    if (_insert_in_progress)
      return;
    write_some_async();
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

