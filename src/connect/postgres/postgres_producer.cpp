#include <kspp/connect/postgres/postgres_producer.h>
#include <chrono>
#include <memory>
#include <glog/logging.h>
#include <boost/bind.hpp>
#include <kspp/kspp.h>
#include <kspp/connect/postgres/postgres_avro_utils.h>

using namespace std::chrono_literals;

namespace kspp {
  postgres_producer::postgres_producer(std::string table,
                                       const kspp::connect::connection_params& cp,
                                       std::string id_column,
                                       std::string client_encoding,
                                       size_t max_items_in_fetch )
      : _good(true)
      , _closed(false)
      , _start_running(false)
      , _connected(false)
      , _exit(false)
      , _bg([this] { _thread(); })
      , _connection(std::make_shared<kspp_postgres::connection>())
      , _table(table)
      , cp_(cp)
      , _id_column(id_column)
      , _client_encoding(client_encoding)
      , _max_items_in_fetch(max_items_in_fetch)
      , _msg_cnt(0)
      , _msg_bytes(0)
      , _table_checked(false)
      , _table_exists(false) {
    initialize();
  }

  postgres_producer::~postgres_producer(){
    _exit = true;
    if (!_closed)
      close();
    _bg.join();
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

  void postgres_producer::stop(){

  }

  bool postgres_producer::initialize() {
    if (_connection->connect(cp_)){
      LOG(ERROR) << "could not connect to " << cp_.host;
      return false;
    }

    if (_connection->set_client_encoding(_client_encoding)){
      LOG(ERROR) << "could not set client encoding " << _client_encoding;
      return false;
    }

    check_table_exists();

    _start_running = true;
  }


  /*
  SELECT EXISTS (
   SELECT 1
   FROM   pg_tables
   WHERE  schemaname = 'schema_name'
   AND    tablename = 'table_name'
   );
   */

  bool postgres_producer::check_table_exists() {
    std::string statement = "SELECT 1 FROM pg_tables WHERE tablename = '" + _table + "'";
    DLOG(INFO) << "exec(" + statement + ")";
    auto res = _connection->exec(statement);

    if (res.first) {
      LOG(FATAL) << statement  << " failed ec:" << res.first << " last_error: " << _connection->last_error();
      _table_checked = true;
      return false;
    }

    int tuples_in_batch = PQntuples(res.second.get());

    if(tuples_in_batch>0) {
      LOG(INFO) << _table << " exists";
      _table_exists = true;
    } else {
      LOG(INFO) << _table << " not existing - will be created later";
    }

    _table_checked = true;
    return true;
  }

  void postgres_producer::_thread() {

    while (!_exit) {
      if (_closed)
        break;

      // connected
      if (!_start_running) {
        std::this_thread::sleep_for(1s);
        continue;
      }

      // have we lost connection ?
      if (!_connection->connected()) {
        if (!_connection->connect(cp_))
        {
          std::this_thread::sleep_for(10s);
          continue;
        }

        if (!_connection->set_client_encoding(_client_encoding)){
          std::this_thread::sleep_for(10s);
          continue;
        }
      }

      if (_incomming_msg.empty()){
        std::this_thread::sleep_for(100ms);
        continue;
      }

      if (!_table_exists) {
        auto msg = _incomming_msg.front();
        // do not do this if this is a delete message
        if (msg->record()->value()) {
          //TODO verify that the data actually has the _id_column(s)
          std::string statement = pq::avro2sql_create_table_statement(_table, _id_column,
                                                                  *msg->record()->value()->valid_schema());
          LOG(INFO) << "exec(" + statement + ")";
          auto res = _connection->exec(statement);

// TODO!!!!!
//        if (ec) {
//            LOG(FATAL) << statement << " failed ec:" << ec << " last_error: " << _connection->last_error();
//            _table_checked = true;
//        } else {
//          _table_exists = true;
//        };
          _table_exists = true;
        }
      }


      auto msg = _incomming_msg.front();
      // upsert?
      if (msg->record()->value()) {
        std::string statement = pq::avro2sql_build_insert_1(_table, *msg->record()->value()->valid_schema());
        std::string upsert_part = pq::avro2sql_build_upsert_2(_table, _id_column, *msg->record()->value()->valid_schema());

        size_t msg_in_batch = 0;
        size_t bytes_in_batch = 0;
        std::set<std::string> unique_keys_in_batch;
        event_queue<kspp::generic_avro, kspp::generic_avro> in_batch;
        while (!_incomming_msg.empty() && msg_in_batch < _max_items_in_fetch) {
          auto msg = _incomming_msg.front();
          if (msg->record()->value()==nullptr) {
            DLOG(INFO) << "breaking up upsert due to delete message, batch size: " << msg_in_batch;
            break;
          }

          // we cannot have the id columns of this update more than once
          // postgres::exec failed ERROR:  ON CONFLICT DO UPDATE command cannot affect row a second time
          auto key_string = pq::avro2sql_key_values(*msg->record()->value()->valid_schema(), _id_column,
                                                *msg->record()->value()->generic_datum());
          auto res = unique_keys_in_batch.insert(key_string);
          if (res.second == false) {
            DLOG(INFO)
                << "breaking up upsert due to 'ON CONFLICT DO UPDATE command cannot affect row a second time...' batch size: "
                << msg_in_batch;
            break;
          }

          if (msg_in_batch > 0)
            statement += ", \n";
          statement += pq::avro2sql_values(*msg->record()->value()->valid_schema(),
                                       *msg->record()->value()->generic_datum());
          ++msg_in_batch;
          in_batch.push_back(msg);
          _incomming_msg.pop_front();
        }

        statement += "\n";
        statement += upsert_part;
        bytes_in_batch += statement.size();
        //std::cerr << statement << std::endl;

        auto ts0 = kspp::milliseconds_since_epoch();
        auto res = _connection->exec(statement);
        auto ts1 = kspp::milliseconds_since_epoch();


        /*
         * if (ec) {
          LOG(FATAL) << statement << " failed ec:" << ec << " last_error: "
                     << _connection->last_error();
          return;
        }
        */

        while (!in_batch.empty()) {
          _done.push_back(in_batch.pop_and_get());
        }

        _msg_cnt += msg_in_batch;
        _msg_bytes += bytes_in_batch;
      } else {
        std::string statement = "DELETE FROM " + _table + " WHERE ";
        size_t msg_in_batch = 0;
        size_t bytes_in_batch = 0;
        event_queue<kspp::generic_avro, kspp::generic_avro> in_batch;
        while (!_incomming_msg.empty() && msg_in_batch < _max_items_in_fetch) {
          auto msg = _incomming_msg.front();
          if (msg->record()->value() != nullptr) {
            DLOG(INFO) << "breaking up delete due to insert message, batch size: " << msg_in_batch;
            break;
          }
          if (msg_in_batch > 0)
            statement += "OR \n ";
          statement += pq::avro2sql_delete_key_values(*msg->record()->key().valid_schema(), _id_column, *msg->record()->key().generic_datum());
          ++msg_in_batch;
          in_batch.push_back(msg);
          _incomming_msg.pop_front();
        }
        statement += "\n";

         // special case - no need to delete stuff from n on existent table
        if (_table_exists) {
          //LOG(INFO) << statement;
          bytes_in_batch += statement.size();
          auto ts0 = kspp::milliseconds_since_epoch();
          auto res = _connection->exec(statement);
          auto ts1 = kspp::milliseconds_since_epoch();
        }
        /*
         * if (ec) {
          LOG(FATAL) << statement << " failed ec:" << ec << " last_error: "
                     << _connection->last_error();
          return;
        }
        */

        while (!in_batch.empty()) {
          _done.push_back(in_batch.pop_and_get());
        }

        _msg_cnt += msg_in_batch;
        //_msg_bytes += bytes_in_batch;
      }

      }
  DLOG(INFO) << "exiting thread";
}


void postgres_producer::poll() {
  while (!_done.empty()) {
    _done.pop_front();
  }
}

}

