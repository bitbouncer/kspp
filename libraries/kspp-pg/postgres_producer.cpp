#include <kspp-pg/postgres_producer.h>
#include <chrono>
#include <memory>
#include <glog/logging.h>
#include <kspp/kspp.h>
#include <kspp-pg/postgres_avro_utils.h>

using namespace std::chrono_literals;

namespace kspp {
  postgres_producer::postgres_producer(std::string table,
                                       const kspp::connect::connection_params &cp,
                                       std::vector<std::string> id_columns,
                                       std::string client_encoding,
                                       size_t max_items_in_insert,
                                       bool skip_delete)
      : connection_(std::make_unique<kspp_postgres::connection>())
        , table_(table)
        , cp_(cp)
        , id_columns_(id_columns)
        , client_encoding_(client_encoding)
        , max_items_in_insert_(max_items_in_insert)
        , skip_delete2_(skip_delete)
        , connection_errors_("connection_errors", "msg")
        , insert_errors_("insert_errors", "msg")
        , msg_cnt_("inserted", "msg")
        , msg_bytes_("bytes_sent", "bytes")
        , request_time_("pg_request_time", "ms", {0.9, 0.99})
        , bg_([this] { _thread(); }) {
    initialize();
  }

  postgres_producer::~postgres_producer() {
    exit_ = true;
    if (!closed_)
      close();
    bg_.join();
    connection_->close();
    connection_.reset(nullptr);
  }

  void postgres_producer::register_metrics(kspp::processor *parent) {
    parent->add_metric(&connection_errors_);
    parent->add_metric(&insert_errors_);
    parent->add_metric(&msg_cnt_);
    parent->add_metric(&msg_bytes_);
    parent->add_metric(&request_time_);
  }

  void postgres_producer::close() {
    if (closed_)
      return;
    closed_ = true;
    if (connection_) {
      connection_->close();
      LOG(INFO) << "postgres_producer table:" << table_ << ", closed - producer " << (int64_t) msg_cnt_.value()
                << " messages (" << (int64_t) msg_bytes_.value() << " bytes)";
    }
  }

  void postgres_producer::stop() {

  }

  bool postgres_producer::initialize() {
    if (connection_->connect(cp_)) {
      LOG(ERROR) << "could not connect to " << cp_.host;
      return false;
    }

    if (connection_->set_client_encoding(client_encoding_)) {
      LOG(ERROR) << "could not set client encoding " << client_encoding_;
      return false;
    }

    check_table_exists();
    start_running_ = true;
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
    std::string statement = "SELECT 1 FROM pg_tables WHERE tablename = '" + table_ + "'";
    DLOG(INFO) << "exec(" + statement + ")";
    auto res = connection_->exec(statement);

    if (res.first) {
      LOG(FATAL) << statement << " failed ec:" << res.first << " last_error: " << connection_->last_error();
      table_checked_ = true;
      return false;
    }

    int tuples_in_batch = PQntuples(res.second.get());

    if (tuples_in_batch > 0) {
      LOG(INFO) << table_ << " exists";
      table_exists_ = true;
    } else {
      LOG(INFO) << table_ << " not existing - will be created later";
    }

    table_checked_ = true;
    return true;
  }

  void postgres_producer::_thread() {

    while (!exit_) {
      if (closed_)
        break;

      // connected
      if (!start_running_) {
        std::this_thread::sleep_for(1s);
        continue;
      }

      // have we lost connection ?
      if (!connection_->connected()) {
        if (!connection_->connect(cp_)) {
          ++connection_errors_;
          ++current_error_streak_;
          std::this_thread::sleep_for(10s);
          continue;
        }

        if (!connection_->set_client_encoding(client_encoding_)) {
          std::this_thread::sleep_for(10s);
          continue;
        }
      }

      if (incomming_msg_.empty()) {
        std::this_thread::sleep_for(100ms);
        continue;
      }

      if (!table_exists_) {
        auto msg = incomming_msg_.front();
        // do not do this if this is a delete message
        if (msg->record()->value()) {
          //TODO verify that the data actually has the _id_column(s)
          std::string statement = pq::avro2sql_create_table_statement(table_, id_columns_,
                                                                      *msg->record()->value()->valid_schema());
          LOG(INFO) << "exec(" + statement + ")";
          auto res = connection_->exec(statement);

// TODO!!!!!
//        if (ec) {
//            LOG(FATAL) << statement << " failed ec:" << ec << " last_error: " << _connection->last_error();
//            _table_checked = true;
//        } else {
//          _table_exists = true;
//        };
          table_exists_ = true;
        }
      }


      auto msg = incomming_msg_.front();

      if (skip_delete2_ && msg->record()->value() == nullptr) {
        done_.push_back(msg);
        incomming_msg_.pop_front();
        DLOG(INFO) << "skipping delete";
        continue;
      }

      // upsert?
      if (msg->record()->value()) {
        std::string statement = pq::avro2sql_build_insert_1(table_, *msg->record()->value()->valid_schema());
        std::string upsert_part = pq::avro2sql_build_upsert_2(table_, id_columns_,
                                                              *msg->record()->value()->valid_schema());

        size_t msg_in_batch = 0;
        size_t bytes_in_batch = 0;
        //size_t same_key_skipped=0;
        std::set<std::string> unique_keys_in_batch;
        std::map<std::string, int64_t> unique_keys_in_batch2;
        std::deque<std::shared_ptr<kevent<kspp::generic_avro, kspp::generic_avro>>> in_update_batch;
        while (!incomming_msg_.empty() && msg_in_batch < max_items_in_insert_) {
          auto msg = incomming_msg_.front();
          if (msg->record()->value() == nullptr) {
            if (skip_delete2_) {
              DLOG(INFO) << "skipping delete";
              done_.push_back(msg);
              incomming_msg_.pop_front();
              continue;
            }

            DLOG(INFO) << "breaking up upsert due to delete message, batch size: " << msg_in_batch;
            break;
          }

          // we cannot have the id columns of this update more than once
          // postgres::exec failed ERROR:  ON CONFLICT DO UPDATE command cannot affect row a second time
          auto key_string = pq::avro2sql_key_values(*msg->record()->value()->valid_schema(), id_columns_,
                                                    *msg->record()->value()->generic_datum());
          //LOG(INFO) << "key string " << key_string;

          auto res0 = unique_keys_in_batch2.find(key_string);
          if (res0 != unique_keys_in_batch2.end()) {
            if (res0->second == msg->event_time()) {
              done_.push_back(msg);
              incomming_msg_.pop_front();
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
          statement += pq::avro2sql_values(*msg->record()->value()->valid_schema(),
                                           *msg->record()->value()->generic_datum());
          //LOG(INFO) << statement;
          ++msg_in_batch;
          in_update_batch.push_back(msg);
          incomming_msg_.pop_front();
        }

        statement += "\n";
        statement += upsert_part;
        bytes_in_batch += statement.size();
        //std::cerr << statement << std::endl;

        auto ts0 = kspp::milliseconds_since_epoch();
        auto res = connection_->exec(statement);
        auto ts1 = kspp::milliseconds_since_epoch();

        // if we failed we have to push back messages to the _incomming_msg and retry
        if (res.first) {
          ++insert_errors_;
          ++current_error_streak_;
          // should we just exit here ??? - it depends if we trust stored offsets.
          //DLOG(INFO) << statement;

          while (!in_update_batch.empty()) {
            //LOG(INFO) << "pushing back failed update to queue";

            incomming_msg_.push_front(in_update_batch.back());
            in_update_batch.pop_back();
          }
        } else {
          current_error_streak_ = 0;

          while (!in_update_batch.empty()) {
            done_.push_back(in_update_batch.back());
            in_update_batch.pop_back();
          }
          request_time_.observe(ts1 - ts0);
          msg_cnt_ += msg_in_batch;
          msg_bytes_ += bytes_in_batch;
        }

        if (!in_update_batch.empty())
          LOG(ERROR) << "in_batch should be empty here";
      } else {
        std::string statement = "DELETE FROM " + table_ + " WHERE ";
        size_t msg_in_batch = 0;
        size_t bytes_in_batch = 0;
        std::deque<std::shared_ptr<kevent<kspp::generic_avro, kspp::generic_avro>>> in_delete_batch;
        while (!incomming_msg_.empty() && msg_in_batch < max_items_in_insert_) {
          // TODO should proably be something different from insert limit  - postpone till we do out-of-band" delete
          auto msg = incomming_msg_.front();
          if (msg->record()->value() != nullptr) {
            DLOG(INFO) << "breaking up delete due to insert message, batch size: " << msg_in_batch;
            break;
          }
          if (msg_in_batch > 0)
            statement += "OR \n ";
          statement += pq::avro2sql_delete_key_values(*msg->record()->key().valid_schema(), id_columns_,
                                                      *msg->record()->key().generic_datum());
          ++msg_in_batch;
          in_delete_batch.push_back(msg);
          incomming_msg_.pop_front();
        }
        statement += "\n";

        // special case - no need to delete stuff from n on existent table
        if (table_exists_) {
          //LOG(INFO) << statement;
          bytes_in_batch += statement.size();
          auto ts0 = kspp::milliseconds_since_epoch();
          auto res = connection_->exec(statement);
          auto ts1 = kspp::milliseconds_since_epoch();
          // if we failed we have to push back messages to the _incomming_msg and retry
          if (res.first) {
            ++current_error_streak_;
            // should we just exit here ??? - it depends if we trust stored offsets.
            while (!in_delete_batch.empty()) {
              LOG(INFO) << "pushing back failed delete to queue";
              incomming_msg_.push_front(in_delete_batch.back());
              in_delete_batch.pop_back();
            }
          } else {
            current_error_streak_ = 0;
            while (!in_delete_batch.empty()) {
              done_.push_back(in_delete_batch.back());
              in_delete_batch.pop_back();
            }
            request_time_.observe(ts1 - ts0);
            msg_cnt_ += msg_in_batch;
            msg_bytes_ += bytes_in_batch;
          }
        } else {
          while (!in_delete_batch.empty()) {
            done_.push_back(in_delete_batch.back());
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
    while (!done_.empty()) {
      done_.pop_front();
    }
  }

}

