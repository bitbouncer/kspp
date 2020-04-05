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
                                       std::vector<std::string> id_columns,
                                       std::string client_encoding,
                                       size_t max_items_in_insert,
                                       bool skip_delete)
      : _good(true)
      , _closed(false)
      , _start_running(false)
      , _connected(false)
      , _exit(false)
      , _bg([this] { _thread(); })
      , _connection(std::make_unique<kspp_postgres::connection>())
      , _table(table)
      , cp_(cp)
      , _id_columns(id_columns)
      , _client_encoding(client_encoding)
      , _max_items_in_insert(max_items_in_insert)
      , _table_checked(false)
      , _table_exists(false)
      , _skip_delete2(skip_delete)
      , _current_error_streak(0)
      , _request_time("pg_request_time", "ms", { 0.9, 0.99 })
      , _connection_errors("connection_errors", "msg")
      , _insert_errors("insert_errors", "msg")
      , _msg_cnt("inserted", "msg")
      , _msg_bytes("bytes_sent", "bytes"){
    initialize();
  }

  postgres_producer::~postgres_producer(){
    _exit = true;
    if (!_closed)
      close();
    _bg.join();
    _connection->close();
    _connection.reset(nullptr);
  }

  void postgres_producer::register_metrics(kspp::processor* parent){
    parent->add_metric(&_connection_errors);
    parent->add_metric(&_insert_errors);
    parent->add_metric(&_msg_cnt);
    parent->add_metric(&_msg_bytes);
    parent->add_metric(&_request_time);
  }

  void postgres_producer::close(){
    if (_closed)
      return;
    _closed = true;

    if (_connection) {
      _connection->close();
      LOG(INFO) << "postgres_producer table:" << _table << ", closed - producer " << (int64_t)_msg_cnt.value() << " messages (" << (int64_t) _msg_bytes.value() << " bytes)";
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
    return true;
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
          ++_connection_errors;
          ++_current_error_streak;
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
          std::string statement = pq::avro2sql_create_table_statement(_table, _id_columns,
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

      if (_skip_delete2 && msg->record()->value()==nullptr) {
        _done.push_back(msg);
        _incomming_msg.pop_front();
        DLOG(INFO) << "skipping delete";
        continue;
      }

      // upsert?
      if (msg->record()->value()) {
        std::string statement = pq::avro2sql_build_insert_1(_table, *msg->record()->value()->valid_schema());
        std::string upsert_part = pq::avro2sql_build_upsert_2(_table, _id_columns, *msg->record()->value()->valid_schema());

        size_t msg_in_batch = 0;
        size_t bytes_in_batch = 0;
        //size_t same_key_skipped=0;
        std::set<std::string> unique_keys_in_batch;
        std::map<std::string, int64_t> unique_keys_in_batch2;
        std::deque<std::shared_ptr<kevent<kspp::generic_avro, kspp::generic_avro>>> in_update_batch;
        while (!_incomming_msg.empty() && msg_in_batch < _max_items_in_insert) {
          auto msg = _incomming_msg.front();
          if (msg->record()->value()==nullptr) {
            if (_skip_delete2) {
              DLOG(INFO) << "skipping delete";
              _done.push_back(msg);
              _incomming_msg.pop_front();
              continue;
            }

            DLOG(INFO) << "breaking up upsert due to delete message, batch size: " << msg_in_batch;
            break;
          }

          // we cannot have the id columns of this update more than once
          // postgres::exec failed ERROR:  ON CONFLICT DO UPDATE command cannot affect row a second time
          auto key_string = pq::avro2sql_key_values(*msg->record()->value()->valid_schema(), _id_columns,
                                                    *msg->record()->value()->generic_datum());
          //LOG(INFO) << "key string " << key_string;

          auto res0 = unique_keys_in_batch2.find(key_string);
          if (res0 != unique_keys_in_batch2.end()){
            if (res0->second == msg->event_time()){
              _done.push_back(msg);
              _incomming_msg.pop_front();
              continue;
              //same_key_skipped++;
            }
          }

          unique_keys_in_batch2[key_string] = msg->event_time();

          auto res = unique_keys_in_batch.insert(key_string);
          if (res.second == false) {
            DLOG(INFO)
                << "breaking up upsert due to 'ON CONFLICT DO UPDATE command cannot affect row a second time...' batch size: "
                << msg_in_batch;
            break;
          }

          if (msg_in_batch > 0)
            statement += ", \n";
          statement += pq::avro2sql_values(*msg->record()->value()->valid_schema(), *msg->record()->value()->generic_datum());
          //LOG(INFO) << statement;
          ++msg_in_batch;
          in_update_batch.push_back(msg);
          _incomming_msg.pop_front();
        }

        statement += "\n";
        statement += upsert_part;
        bytes_in_batch += statement.size();
        //std::cerr << statement << std::endl;

        auto ts0 = kspp::milliseconds_since_epoch();
        auto res = _connection->exec(statement);
        auto ts1 = kspp::milliseconds_since_epoch();

        // if we failed we have to push back messages to the _incomming_msg and retry
        if (res.first){
          ++_insert_errors;
          ++_current_error_streak;
          // should we just exit here ??? - it depends if we trust stored offsets.
          //DLOG(INFO) << statement;

          while (!in_update_batch.empty()) {
            //LOG(INFO) << "pushing back failed update to queue";

            _incomming_msg.push_front(in_update_batch.back());
            in_update_batch.pop_back();
          }
        } else {
          _current_error_streak=0;

          while (!in_update_batch.empty()) {
            _done.push_back(in_update_batch.back());
            in_update_batch.pop_back();
          }
          _request_time.observe(ts1-ts0);
          _msg_cnt += msg_in_batch;
          _msg_bytes += bytes_in_batch;
        }

        if (!in_update_batch.empty())
          LOG(ERROR) << "in_batch should be empty here";
      } else {
        std::string statement = "DELETE FROM " + _table + " WHERE ";
        size_t msg_in_batch = 0;
        size_t bytes_in_batch = 0;
        std::deque<std::shared_ptr<kevent<kspp::generic_avro, kspp::generic_avro>>> in_delete_batch;
        while (!_incomming_msg.empty() && msg_in_batch < _max_items_in_insert) { // TODO should proably be something different from insert limit  - postpone till we do out-of-band" delete
          auto msg = _incomming_msg.front();
          if (msg->record()->value() != nullptr) {
            DLOG(INFO) << "breaking up delete due to insert message, batch size: " << msg_in_batch;
            break;
          }
          if (msg_in_batch > 0)
            statement += "OR \n ";
          statement += pq::avro2sql_delete_key_values(*msg->record()->key().valid_schema(), _id_columns, *msg->record()->key().generic_datum());
          ++msg_in_batch;
          in_delete_batch.push_back(msg);
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
          // if we failed we have to push back messages to the _incomming_msg and retry
          if (res.first) {
            ++_current_error_streak;
            // should we just exit here ??? - it depends if we trust stored offsets.
            while (!in_delete_batch.empty()) {
              LOG(INFO) << "pushing back failed delete to queue";
              _incomming_msg.push_front(in_delete_batch.back());
              in_delete_batch.pop_back();
            }
          } else {
            _current_error_streak=0;
            while (!in_delete_batch.empty()) {
              _done.push_back(in_delete_batch.back());
              in_delete_batch.pop_back();
            }
            _request_time.observe(ts1-ts0);
            _msg_cnt += msg_in_batch;
            _msg_bytes += bytes_in_batch;
          }
        } else  {
          while (!in_delete_batch.empty()) {
            _done.push_back(in_delete_batch.back());
            in_delete_batch.pop_back();
          }
        }
        if (!in_delete_batch.empty())
          LOG(ERROR) << "in_batch should be empty here";
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

