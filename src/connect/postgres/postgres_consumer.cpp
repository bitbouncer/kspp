#include <kspp/connect/postgres/postgres_consumer.h>
#include <kspp/kspp.h>
#include <chrono>
#include <memory>
#include <glog/logging.h>
#include <boost/bind.hpp>
#include <kspp/connect/postgres/postgres_avro_utils.h>

using namespace std::chrono_literals;

namespace kspp {

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
      if (record.fieldAt(j).type() != avro::AVRO_UNION) // this should not hold - but we fail to create correct schemas for not null columns
      {
        LOG(FATAL) << "unexpected schema - bailing out, type:" << record.fieldAt(j).type();
        break;
      }
      avro::GenericUnion& au(record.fieldAt(j).value<avro::GenericUnion>());

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
        au.selectBranch(0); // NULL branch - we hope..
        assert(au.datum().type() == avro::AVRO_NULL);
      }
      else
      {
        au.selectBranch(1);
        avro::GenericDatum& avro_item(au.datum());
        const char* val = PQgetvalue(pgres, row, j);

        switch (avro_item.type())
        {
          case avro::AVRO_STRING:
            avro_item.value<std::string>() = val;
            break;
          case avro::AVRO_BYTES:
            avro_item.value<std::string>() = val;
            break;
          case avro::AVRO_INT:
            avro_item.value<int32_t>() = atoi(val);
            break;
          case avro::AVRO_LONG:
            avro_item.value<int64_t>() = std::stoull(val);
            break;
          case avro::AVRO_FLOAT:
            avro_item.value<float>() = (float)atof(val);
            break;
          case avro::AVRO_DOUBLE:
            avro_item.value<double>() = atof(val);
            break;
          case avro::AVRO_BOOL:
            avro_item.value<bool>() = (val[0]=='t' || val[0]=='T' || val[0]=='1');
            break;
          case avro::AVRO_RECORD:
          case avro::AVRO_ENUM:
          case avro::AVRO_ARRAY:
          case avro::AVRO_MAP:
          case avro::AVRO_UNION:
          case avro::AVRO_FIXED:
          case avro::AVRO_NULL:
          default:
            LOG(FATAL) << "unexpected / non supported type e:" << avro_item.type();
        }
      }
    }
  }

  postgres_consumer::postgres_consumer(int32_t partition,
                                       std::string logical_name,
                                       std::string consumer_group,
                                       const kspp::connect::connection_params& cp,
                                       std::string query,
                                       std::string id_column,
                                       std::string ts_column,
                                       std::shared_ptr<kspp::avro_schema_registry> schema_registry,
                                       std::chrono::seconds poll_intervall,
                                       size_t max_items_in_fetch)
      : _good(true)
      , _closed(false)
      , _eof(false)
      , _start_running(false)
      , _exit(false)
      , _bg([this] { _thread(); })
      , _connection(std::make_shared<kspp_postgres::connection>())
      , _logical_name(logical_name)
      , _partition(partition)
      , _consumer_group(consumer_group)
      , cp_(cp)
      , _query(query)
      , _id_column(id_column)
      , _ts_column(ts_column)
      , id_column_index_(-1)
      , ts_column_index_(-1)
      , schema_registry_(schema_registry)
      , _max_items_in_fetch(max_items_in_fetch)
      , key_schema_id_(-1)
      , value_schema_id_(-1)
      , poll_intervall_(poll_intervall)
      , _msg_cnt(0) {
  }

  postgres_consumer::~postgres_consumer() {
    _exit = true;
    if (!_closed)
      close();
    _bg.join();
    _connection->close();
    _connection = nullptr;
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
          key_schema_ = std::make_shared<avro::ValidSchema>(*schema_for_table_key(_logical_name, {_id_column}, result.get()));
          std::string simple_name = simple_column_name(_id_column);
          id_column_index_ = PQfnumber(result.get(), simple_name.c_str());
        }

        if (schema_registry_) {
          key_schema_id_ = schema_registry_->put_schema(_logical_name + "_key", key_schema_);
        }

        std::stringstream ss0;
        key_schema_->toJson(ss0);
        LOG(INFO) << "key_schema: \n" << ss0.str();
      }

      value_schema_ = std::make_shared<avro::ValidSchema>(*schema_for_table_row(_logical_name, result.get()));
      if (schema_registry_) {
        // we should probably prepend the name with a prefix (like _my_db_table_name)
        value_schema_id_ = schema_registry_->put_schema(_logical_name + "_value", value_schema_);
      }

      std::stringstream ss1;
      value_schema_->toJson(ss1);
      LOG(INFO) << "value_schema: \n" << ss1.str();

      // might be in form a.ts (get rid of a.)
      std::string simple_name = simple_column_name(_ts_column);
      ts_column_index_ = PQfnumber(result.get(), simple_name.c_str());
    }

    int nRows = PQntuples(result.get());

    for (int i = 0; i < nRows; i++) {
      std::shared_ptr<kspp::generic_avro> key;

      key = std::make_shared<kspp::generic_avro>(key_schema_, key_schema_id_);
      load_avro_by_name(key.get(), result.get(), i);

      auto value = std::make_shared<kspp::generic_avro>(value_schema_, value_schema_id_);

      load_avro_by_name(value.get(), result.get(), i);

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

      auto record = std::make_shared<krecord<kspp::generic_avro, kspp::generic_avro>>(*key, value, kspp::milliseconds_since_epoch());
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

      std::string where_clause;

      // do we have a timestamp field
      // we have to have either a interger id that is increasing or a timestamp that is increasing
      // before we read anything the where clause will not be valid
      if (last_id_.size()) {
        if (_ts_column.size())
          where_clause =
              " WHERE (" + _ts_column + " = '" + last_ts_ + "' AND " + _id_column + " > '" + last_id_ + "') OR (" +
              _ts_column + " > '" + last_ts_ + "')";
        else
          where_clause = " WHERE " + _id_column + " > '" + last_id_ + "'";
      } else {
        if (last_ts_.size()) {
          if (cp_.connect_ts_policy == kspp::connect::GREATER) // this leads to potential data loss
            where_clause = " WHERE " + _ts_column + " > '" + last_ts_ + "'";
          else if (cp_.connect_ts_policy == kspp::connect::GREATER_OR_EQUAL) // this leads to duplicates since last is repeated
            where_clause = " WHERE " + _ts_column + " >= '" + last_ts_ + "'";
          else
            LOG(FATAL) << "unknown cp_.connect_ts_policy: " << cp_.connect_ts_policy;
        }
      }
      std::string statement = _query + where_clause + order_by + " LIMIT " + std::to_string(_max_items_in_fetch);

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

      if ((_msg_cnt - last_msg_count) != _max_items_in_fetch)
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
        // what is sleeping cannot bne killed...
        int count = poll_intervall_.count();
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

