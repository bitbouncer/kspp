#include <kspp/connect/postgres/postgres_consumer.h>
#include <kspp/kspp.h>
#include <chrono>
#include <memory>
#include <glog/logging.h>
#include <boost/bind.hpp>
#include <kspp/connect/postgres/postgres_avro_utils.h>

using namespace std::chrono_literals;

namespace kspp {
  // trim from left
  static inline std::string& ltrim(std::string& s, const char* t = " \t\n\r\f\v")
  {
    s.erase(0, s.find_first_not_of(t));
    return s;
  }

// trim from right
  static  inline std::string& rtrim(std::string& s, const char* t = " \t\n\r\f\v")
  {
    s.erase(s.find_last_not_of(t) + 1);
    return s;
  }

// trim from left & right
  static  inline std::string& trim(std::string& s, const char* t = " \t\n\r\f\v")
  {
    return ltrim(rtrim(s, t), t);
  }




  static void load_avro_by_name(kspp::generic_avro* avro, PGresult* pgres, size_t row)
  {
    // key tupe is null if there is no key
    if (avro->type() == avro::AVRO_NULL)
      return;

    assert(avro->type() == avro::AVRO_RECORD);
    avro::GenericRecord& record(avro->generic_datum()->value<avro::GenericRecord>());
    size_t nFields = record.fieldCount();
    for (int j = 0; j < nFields; j++)
    {
      avro::GenericDatum& col = record.fieldAt(j); // expected union
      if (!record.fieldAt(j).isUnion()) // this should not hold - but we fail to create correct schemas for not null columns
      {
        LOG(INFO) << avro->valid_schema()->toJson();
        LOG(FATAL) << "unexpected schema - bailing out, type:" << record.fieldAt(j).type();
        break;
      }

      //avro::GenericUnion& au(record.fieldAt(j).value<avro::GenericUnion>());

      const std::string& column_name = record.schema()->nameAt(j);

      //which pg column has this value?
      int column_index = PQfnumber(pgres, column_name.c_str());
      if (column_index < 0)
      {
        LOG(FATAL) << "unknown column - bailing out: " << column_name;
        break;
      }

      if (PQgetisnull(pgres, row, column_index) == 1)
      {
        col.selectBranch(0); // NULL branch - we hope..
        assert(col.type() == avro::AVRO_NULL);
      }
      else
      {
        col.selectBranch(1);
        //au.selectBranch(1);
        //avro::GenericDatum& avro_item(au.datum());
        const char* val = PQgetvalue(pgres, row, j);

        switch (col.type()) {
          case avro::AVRO_STRING:
            col.value<std::string>() = val;
            break;
          case avro::AVRO_BYTES:
            col.value<std::string>() = val;
            break;
          case avro::AVRO_INT:
            col.value<int32_t>() = atoi(val);
            break;
          case avro::AVRO_LONG:
            col.value<int64_t>() = std::stoull(val);
            break;
          case avro::AVRO_FLOAT:
            col.value<float>() = (float) atof(val);
            break;
          case avro::AVRO_DOUBLE:
            col.value<double>() = atof(val);
            break;
          case avro::AVRO_BOOL:
            col.value<bool>() = (val[0] == 't' || val[0] == 'T' || val[0] == '1');
            break;
          case avro::AVRO_MAP: {
            std::vector<std::string> kvs;
            boost::split(kvs, val, boost::is_any_of(",")); // TODO we cannot handle "dsd,hggg" => "jhgf"

            avro::GenericMap& v = col.value<avro::GenericMap>();
            avro::GenericMap::Value& r = v.value();

            // this is an empty string "" that will be mapped as 1 item of empty size
            if (kvs.size()==1 && kvs[0].size() ==0)
              break;

            r.resize(kvs.size());

            int cursor=0;
            for(auto& i : kvs){
              std::size_t found = i.find("=>");
              if (found==std::string::npos)
                LOG(FATAL) << "expected => in hstore";
              std::string key = i.substr(0, found);
              std::string val = i.substr(found +2);
              trim(key, "\" ");
              trim(val, "\" ");
              r[cursor].first = key;
              r[cursor].second = avro::GenericDatum(val);
              ++cursor;
            }
          }
            break;

          case avro::AVRO_ARRAY:{
            std::vector<std::string> kvs;
            std::string trimmed_val = val;
            trim(trimmed_val, "{ }");
            boost::split(kvs, trimmed_val, boost::is_any_of(",")); // TODO we cannot handle [ "dsd,hg", ljdshf ]
            avro::GenericArray& v = col.value<avro::GenericArray>();
            avro::GenericArray::Value& r = v.value();

            // this is an empty string "" that will be mapped as 1 item of empty size
            if (kvs.size()==1 && kvs[0].size() ==0)
              break;

            r.resize(kvs.size());

            int cursor=0;
            for(auto& i : kvs) {
              r[cursor] = avro::GenericDatum(i);
              ++cursor;
            }

            }
          break;

          case avro::AVRO_RECORD:
          case avro::AVRO_ENUM:
          case avro::AVRO_UNION:
          case avro::AVRO_FIXED:
          case avro::AVRO_NULL:
          default:
            LOG(FATAL) << "unexpected / non supported type e:" << col.type();
        }
      }
    }
  }

  postgres_consumer::postgres_consumer(int32_t partition,
                                       std::string logical_name,
                                       std::string consumer_group,
                                       const kspp::connect::connection_params& cp,
                                       kspp::connect::table_params tp,
                                       std::string query,
                                       std::string id_column,
                                       std::string ts_column,
                                       std::shared_ptr<kspp::avro_schema_registry> schema_registry)
      : _good(true)
      , _closed(false)
      , _eof(false)
      , _start_running(false)
      , _exit(false)
      , _bg([this] { _thread(); })
      , _connection(std::make_unique<kspp_postgres::connection>())
      , _logical_name(logical_name)
      , _partition(partition)
      , _consumer_group(consumer_group)
      , cp_(cp)
      , tp_(tp)
      , _query(query)
      , _id_column(id_column)
      , _ts_column(ts_column)
      , id_column_index_(-1)
      , ts_column_index_(-1)
      , schema_registry_(schema_registry)
      , key_schema_id_(-1)
      , value_schema_id_(-1)
      , _msg_cnt(0) {
  }

  postgres_consumer::~postgres_consumer() {
    _exit = true;
    if (!_closed)
      close();
    _bg.join();
    _connection->close();
    _connection.reset(nullptr);
  }

  void postgres_consumer::close() {
    _exit = true;
    _start_running = false;

    if (_closed)
      return;
    _closed = true;

    if (_connection) {
      _connection->close();
      LOG(INFO) << "postgres_consumer table:" << _logical_name << ":" << _partition << ", closed - consumed " << _msg_cnt << " messages";
    }
  }

  bool postgres_consumer::initialize() {
    if (_connection->connect(cp_)){
      LOG(ERROR) << "could not connect to " << cp_.host;
      return false;
    }

    if (_connection->set_client_encoding("UTF8")){
      LOG(ERROR) << "could not set client encoding UTF8 ";
      return false;
    }

    //should we check more thing in database
    //maybe select a row and register the schema???

    _start_running = true;
  }

  void postgres_consumer::start(int64_t offset) {
    if (offset == kspp::OFFSET_STORED) {
      //TODO not implemented yet
      /*
       * if (_config->get_cluster_metadata()->consumer_group_exists(_consumer_group, 5s)) {
        DLOG(INFO) << "kafka_consumer::start topic:" << _topic << ":" << _partition  << " consumer group: " << _consumer_group << " starting from OFFSET_STORED";
      } else {
        //non existing consumer group means start from beginning
        LOG(INFO) << "kafka_consumer::start topic:" << _topic << ":" << _partition  << " consumer group: " << _consumer_group << " missing, OFFSET_STORED failed -> starting from OFFSET_BEGINNING";
        offset = kspp::OFFSET_BEGINNING;
      }
       */
    } else if (offset == kspp::OFFSET_BEGINNING) {
      DLOG(INFO) << "postgres_consumer::start table:" << _logical_name << ":" << _partition << " consumer group: "
                 << _consumer_group << " starting from OFFSET_BEGINNING";
    } else if (offset == kspp::OFFSET_END) {
      DLOG(INFO) << "postgres_consumer::start table:" << _logical_name << ":" << _partition << " consumer group: "
                 << _consumer_group << " starting from OFFSET_END";
    } else {
      DLOG(INFO) << "postgres_consumer::start table:" << _logical_name << ":" << _partition << " consumer group: "
                 << _consumer_group << " starting from fixed offset: " << offset;
    }

    initialize();
  }

  int postgres_consumer::parse_response(std::shared_ptr<PGresult> result){
    if (!result)
      return -1;

    // first time?
    if (!value_schema_) {

      if (!key_schema_) {

        if (_id_column.size() == 0) {
          key_schema_ = std::make_shared<avro::ValidSchema>(avro::NullSchema());
        } else {
          key_schema_ = pq::schema_for_table_key(_logical_name + "_key", {_id_column}, result.get());
          std::string simple_name = pq::simple_column_name(_id_column);
          id_column_index_ = PQfnumber(result.get(), simple_name.c_str());
        }

        if (schema_registry_) {
          key_schema_id_ = schema_registry_->put_schema(_logical_name + "-key", key_schema_);
        }

        std::stringstream ss0;
        key_schema_->toJson(ss0);
        LOG(INFO) << "key_schema: \n" << ss0.str();
      }

      value_schema_ = pq::schema_for_table_row(_logical_name + "_value", result.get());
      if (schema_registry_) {
        // we should probably prepend the name with a prefix (like _my_db_table_name)
        value_schema_id_ = schema_registry_->put_schema(_logical_name + "-value", value_schema_);
      }

      std::stringstream ss1;
      value_schema_->toJson(ss1);
      LOG(INFO) << "value_schema: \n" << ss1.str();

      // might be in form a.ts (get rid of a.)
      std::string simple_name = pq::simple_column_name(_ts_column);
      ts_column_index_ = PQfnumber(result.get(), simple_name.c_str());
    }

    int nRows = PQntuples(result.get());

    for (int i = 0; i < nRows; i++) {
      auto key = std::make_shared<kspp::generic_avro>(key_schema_, key_schema_id_);
      load_avro_by_name(key.get(), result.get(), i);
      auto val = std::make_shared<kspp::generic_avro>(value_schema_, value_schema_id_);
      load_avro_by_name(val.get(), result.get(), i);
      // or should we use ts column instead of now();


      if (i == (nRows-1)) {
        //we need to store last timestamp and last key for next select clause
        if (id_column_index_ >= 0)
          last_id_ = PQgetvalue(result.get(), nRows - 1, id_column_index_);
        if (ts_column_index_ >= 0)
          last_ts_ = PQgetvalue(result.get(), nRows - 1, ts_column_index_);

        if (!last_key_)
          last_key_ = std::make_unique<kspp::generic_avro>(key_schema_, key_schema_id_);
        load_avro_by_name(last_key_.get(), result.get(), i);
      }

      auto record = std::make_shared<krecord<kspp::generic_avro, kspp::generic_avro>>(*key, val, kspp::milliseconds_since_epoch());
      auto e = std::make_shared<kevent<kspp::generic_avro, kspp::generic_avro>>(record);
      assert(e.get()!=nullptr);

      //should we wait a bit if we fill incomming queue to much??
      while(_incomming_msg.size()>10000 && !_exit) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        DLOG(INFO) << "c_incomming_msg.size() " << _incomming_msg.size();
      }

      _incomming_msg.push_back(e);
      ++_msg_cnt;
    }

    return 0;
  }


  /*
  std::string postgres_consumer::get_where_clause() const {
    std::string where_clause;

    // if we are rescraping is active at eof - and then we do not care for id at all.
    // rescrape 0 also handles when we do not have an id field
    if (last_ts_> INT_MIN && tp_.rescrape_policy == kspp::connect::LAST_QUERY_TS && _eof)
      return  " WHERE " + _ts_column + " >= '" + std::to_string(last_ts_ - tp_.rescrape_ticks) + "'";

    // no data in either id or ts fields?
    if (last_id_ == INT_MIN || last_ts_ == INT_MIN) {
      if (last_id_ > INT_MIN)
        return " WHERE " + _id_column + " > '" + std::to_string(last_id_) + "'";
      else if (last_ts_ > INT_MIN)
        return " WHERE " + _ts_column + " >= '" + std::to_string(last_ts_) + "'";
      else
        return "";
    } else {
      return " WHERE (" + _ts_column + " = '" + std::to_string(last_ts_) + "' AND " + _id_column + " > '" + std::to_string(last_id_) + "') OR (" + _ts_column + " > '" + std::to_string(last_ts_) + "')";
    }
  }
  */

  std::string postgres_consumer::get_where_clause() const {
    std::string where_clause;

    // if we are rescraping is active at eof - and then we do not care for id at all.
    // rescrape 0 also handles when we do not have an id field
    //if (last_ts_> INT_MIN && tp_.rescrape_policy == kspp::connect::LAST_QUERY_TS && _eof)
    //  return  " WHERE " + _ts_column + " >= '" + std::to_string(last_ts_ - tp_.rescrape_ticks) + "'";

    // no data in either id or ts fields?
    if (last_id_.size()==0 || last_ts_.size()==0) {
      if (last_id_.size()>0)
        return " WHERE " + _id_column + " > '" + last_id_ + "'";
      else if (last_ts_.size()>0)
        return " WHERE " + _ts_column + " >= '" + last_ts_ + "'";
      else
        return "";
    } else {
      return " WHERE (" + _ts_column + " = '" + last_ts_ + "' AND " + _id_column + " > '" + last_id_ + "') OR (" + _ts_column + " > '" + last_ts_ + "')";
    }
  }


  void postgres_consumer::_thread() {
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

        //UTF8?
        if (!_connection->set_client_encoding("UTF8")){
          std::this_thread::sleep_for(10s);
          continue;
        }
      }

      _eof = false;

      std::string order_by = "";
      if (_ts_column.size()) {
        if (_id_column.size())
          order_by = " ORDER BY " + _ts_column + " ASC, " + _id_column + " ASC";
        else
          order_by = " ORDER BY " + _ts_column + " ASC";
      } else {
        order_by = " ORDER BY " + _id_column + " ASC";
      }

      std::string where_clause = get_where_clause();

//      // do we have a timestamp field
//      // we have to have either a interger id that is increasing or a timestamp that is increasing
//      // before we read anything the where clause will not be valid
//      if (last_id_.size()) {
//        if (_ts_column.size())
//          where_clause =
//              " WHERE (" + _ts_column + " = '" + last_ts_ + "' AND " + _id_column + " > '" + last_id_ + "') OR (" +
//              _ts_column + " > '" + last_ts_ + "')";
//        else
//          where_clause = " WHERE " + _id_column + " > '" + last_id_ + "'";
//      } else {
//        if (last_ts_.size()) {
//          if (tp_.connect_ts_policy == kspp::connect::GREATER) // this leads to potential data loss
//            where_clause = " WHERE " + _ts_column + " > '" + last_ts_ + "'";
//          else if (tp_.connect_ts_policy == kspp::connect::GREATER_OR_EQUAL) // this leads to duplicates since last is repeated
//            where_clause = " WHERE " + _ts_column + " >= '" + last_ts_ + "'";
//          else
//            LOG(FATAL) << "unknown cp_.connect_ts_policy: " << tp_.connect_ts_policy;
//        }
//      }

      std::string statement = _query + where_clause + order_by + " LIMIT " + std::to_string(tp_.max_items_in_fetch);

      DLOG(INFO) << "exec(" + statement + ")";

      auto ts0 = kspp::milliseconds_since_epoch();
      auto last_msg_count = _msg_cnt;
      auto res = _connection->exec(statement);
      if (res.first) {
        LOG(ERROR) << "exec failed - disconnecting and retrying e: " << _connection->last_error();
        _connection->disconnect();
        std::this_thread::sleep_for(10s);
        continue;
      }

      int parse_result = parse_response(res.second);
      if (parse_result) {
        LOG(ERROR) << "parse failed - disconnecting and retrying";
        _connection->disconnect();
        std::this_thread::sleep_for(10s);
        continue;
      }
      auto ts1 = kspp::milliseconds_since_epoch();

      if ((_msg_cnt - last_msg_count) != tp_.max_items_in_fetch)
        _eof = true;

      size_t messages_in_batch = _msg_cnt - last_msg_count;

      if (messages_in_batch==0) {
        LOG_EVERY_N(INFO, 100) << "empty poll done, table: " << _logical_name << " total: " << _msg_cnt << ", last ts: "
                               << last_ts_ << " duration " << ts1 - ts0 << " ms";
      }  else {
        LOG(INFO) << "poll done, table: " << _logical_name << " retrieved: " << messages_in_batch << " messages, total: "
                  << _msg_cnt << ", last ts: " << last_ts_ << " duration " << ts1 - ts0 << " ms";
      }

      if (_eof) {
        // what is sleeping cannot be killed...
        int count = tp_.poll_intervall.count();
        for (int i = 0; i != count; ++i) {
          std::this_thread::sleep_for(1s);
          if (_exit)
            break;
        }
      }

    }
    DLOG(INFO) << "exiting thread";
  }
}

