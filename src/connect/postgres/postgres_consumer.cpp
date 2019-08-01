#include <kspp/connect/postgres/postgres_consumer.h>
#include <boost/make_shared.hpp>
#include <chrono>
#include <memory>
#include <glog/logging.h>
#include <boost/bind.hpp>
#include <kspp/kspp.h>
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
                                       const kspp::connect::connection_params& cp,
                                       kspp::connect::table_params tp,
                                       std::string query,
                                       std::string id_column,
                                       std::string ts_column,
                                       std::shared_ptr<kspp::avro_schema_registry> schema_registry)
      : closed_(false)
      , eof_(false)
      , start_running_(false)
      , exit_(false)
      , bg_([this] { _thread(); })
      , connection_(std::make_unique<kspp_postgres::connection>())
      , logical_name_(logical_name)
      , partition_(partition)
      , cp_(cp)
      , tp_(tp)
      , query_(query)
      , read_cursor_(tp, id_column, ts_column)
      , commit_chain_(logical_name, partition)
      , id_column_(id_column)
      , schema_registry_(schema_registry)
      , key_schema_id_(-1)
      , value_schema_id_(-1)
      , _msg_cnt(0) {
    offset_storage_ = get_offset_provider(tp.offset_storage);
  }

  postgres_consumer::~postgres_consumer() {
    exit_ = true;
    if (!closed_)
      close();
    bg_.join();
    commit(true);
    connection_->close();
    connection_.reset(nullptr);
  }

  void postgres_consumer::close() {
    exit_ = true;
    start_running_ = false;

    if (closed_)
      return;
    closed_ = true;

    if (connection_) {
      connection_->close();
      LOG(INFO) << "postgres_consumer table:" << logical_name_ << ":" << partition_ << ", closed - consumed " << _msg_cnt << " messages";
    }
  }

  bool postgres_consumer::initialize() {
    if (connection_->connect(cp_)){
      LOG(ERROR) << "could not connect to " << cp_.host;
      return false;
    }

    if (connection_->set_client_encoding("UTF8")){
      LOG(ERROR) << "could not set client encoding UTF8 ";
      return false;
    }

    // check extensions
    auto res = connection_->exec("SELECT oid FROM pg_type WHERE typname = 'hstore'");

    if (res.first) {
      LOG(ERROR) << "exec failed - disconnecting and retrying e: " << connection_->last_error();
      connection_->disconnect();
      return false;
    }

    if (!res.second)
      return false;

    int nRows = PQntuples(res.second.get());
    if (nRows==1){
      const char* val = PQgetvalue(res.second.get(), 0, 0);
      LOG(INFO) << val;
      int oid = atoi(val);

      auto value_schema = boost::make_shared<avro::MapSchema>(avro::StringSchema());
      boost::shared_ptr<avro::Schema> null_schema = boost::make_shared<avro::NullSchema>();
      boost::shared_ptr<avro::UnionSchema> union_schema = boost::make_shared<avro::UnionSchema>();
      union_schema->addType(*null_schema);
      union_schema->addType(*value_schema);

      extension_oids_[oid] = union_schema; // is is just copy and paste from old code and does not respect not null columns
    }

    //should we check more thing in database
    //maybe select a row and register the schema???
    start_running_ = true;
  }

  void postgres_consumer::start(int64_t offset) {
    int64_t tmp = offset_storage_->start(offset);
    read_cursor_.start(tmp);
    if (tmp>0)
      read_cursor_.set_eof(true); // use rescrape for the first item ie enabled
    initialize();
  }

  std::shared_ptr<avro::ValidSchema> postgres_consumer::schema_for_table_row(std::string schema_name,  const PGresult *res) const {
    avro::RecordSchema record_schema(schema_name);
    int nFields = PQnfields(res);
    for (int i = 0; i < nFields; i++) {
      Oid col_oid = PQftype(res, i);
      std::string col_name = PQfname(res, i);

      boost::shared_ptr<avro::Schema> col_schema;

      auto ext_item = extension_oids_.find(col_oid);
      if (ext_item != extension_oids_.end())
        col_schema = ext_item->second;
      else
        col_schema = pq::schema_for_oid(col_oid); // build in types

      /* TODO ensure that names abide by Avro's requirements */
      record_schema.addField(col_name, *col_schema);
    }
    auto result = std::make_shared<avro::ValidSchema>(record_schema);
    return result;
  }


  int postgres_consumer::parse_response(std::shared_ptr<PGresult> result){
    if (!result)
      return -1;

    // first time?
    if (!value_schema_) {
      read_cursor_.init(result);
      if (!key_schema_) {
        if (id_column_.size() == 0) {
          key_schema_ = std::make_shared<avro::ValidSchema>(avro::NullSchema());
        } else {
          key_schema_ = pq::schema_for_table_key(logical_name_ + "_key", {id_column_}, result.get());
        }

        if (schema_registry_) {
          key_schema_id_ = schema_registry_->put_schema(logical_name_ + "-key", key_schema_);
        }

        std::stringstream ss0;
        key_schema_->toJson(ss0);
        LOG(INFO) << "key_schema: \n" << ss0.str();
      }

      value_schema_ = schema_for_table_row(logical_name_ + "_value", result.get());
      if (schema_registry_) {
        // we should probably prepend the name with a prefix (like _my_db_table_name)
        value_schema_id_ = schema_registry_->put_schema(logical_name_ + "-value", value_schema_);
      }

      std::stringstream ss1;
      value_schema_->toJson(ss1);
      LOG(INFO) << "value_schema: \n" << ss1.str();
    }

    int nRows = PQntuples(result.get());

    for (int i = 0; i < nRows; i++) {
      auto key = std::make_shared<kspp::generic_avro>(key_schema_, key_schema_id_);
      load_avro_by_name(key.get(), result.get(), i);
      auto val = std::make_shared<kspp::generic_avro>(value_schema_, value_schema_id_);
      load_avro_by_name(val.get(), result.get(), i);

      if (i == (nRows-1)) {

        if (!last_key_)
          last_key_ = std::make_unique<kspp::generic_avro>(key_schema_, key_schema_id_);
        load_avro_by_name(last_key_.get(), result.get(), i);
      }

      read_cursor_.parse(result);

      int64_t tick_ms = read_cursor_.last_ts_ms();
      if (tick_ms==0)
        tick_ms = kspp::milliseconds_since_epoch();

      auto record = std::make_shared<krecord<kspp::generic_avro, kspp::generic_avro>>(*key, val, tick_ms);
      // do we have one...
      int64_t tick = read_cursor_.last_tick();
      auto e = std::make_shared<kevent<kspp::generic_avro, kspp::generic_avro>>(record, tick  > 0 ? commit_chain_.create(tick) : nullptr);
      assert(e.get()!=nullptr);

      //auto record = std::make_shared<krecord<kspp::generic_avro, kspp::generic_avro>>(*key, val, kspp::milliseconds_since_epoch());
      //auto e = std::make_shared<kevent<kspp::generic_avro, kspp::generic_avro>>(record);
      //assert(e.get()!=nullptr);

      //should we wait a bit if we fill incomming queue to much??
      while(_incomming_msg.size()>10000 && !exit_) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        DLOG(INFO) << "c_incomming_msg.size() " << _incomming_msg.size();
      }

      _incomming_msg.push_back(e);
      ++_msg_cnt;
    }
    return 0;
  }

  void postgres_consumer::_thread() {
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
        if (!connection_->connect(cp_))
        {
          std::this_thread::sleep_for(10s);
          continue;
        }

        //UTF8?
        if (!connection_->set_client_encoding("UTF8")){
          std::this_thread::sleep_for(10s);
          continue;
        }
      }

      eof_ = false;

      std::string statement = query_ + read_cursor_.get_where_clause() + " LIMIT " + std::to_string(tp_.max_items_in_fetch);

      DLOG(INFO) << "exec(" + statement + ")";
      auto ts0 = kspp::milliseconds_since_epoch();
      auto last_msg_count = _msg_cnt;
      auto res = connection_->exec(statement);
      if (res.first) {
        LOG(ERROR) << "exec failed - disconnecting and retrying e: " << connection_->last_error();
        connection_->disconnect();
        std::this_thread::sleep_for(10s);
        continue;
      }

      int parse_result = parse_response(res.second);
      if (parse_result) {
        LOG(ERROR) << "parse failed - disconnecting and retrying";
        connection_->disconnect();
        std::this_thread::sleep_for(10s);
        continue;
      }
      auto ts1 = kspp::milliseconds_since_epoch();

      if ((_msg_cnt - last_msg_count) != tp_.max_items_in_fetch)
        eof_ = true;

      read_cursor_.set_eof(eof_);

      size_t messages_in_batch = _msg_cnt - last_msg_count;

      if (messages_in_batch==0) {
        LOG_EVERY_N(INFO, 100) << "empty poll done, table: " << logical_name_ << " total: " << _msg_cnt << ", last ts: "
                               << read_cursor_.last_ts() << " duration " << ts1 - ts0 << " ms";
      }  else {
        LOG(INFO) << "poll done, table: " << logical_name_ << " retrieved: " << messages_in_batch << " messages, total: "
                  << _msg_cnt << ", last ts: " << read_cursor_.last_ts() << " duration " << ts1 - ts0 << " ms";
      }

      commit(false);

      if (eof_) {
        // what is sleeping cannot be killed...
        int count = tp_.poll_intervall.count();
        for (int i = 0; i != count; ++i) {
          std::this_thread::sleep_for(1s);
          if (exit_)
            break;
        }
      }

    }
    DLOG(INFO) << "exiting thread";
  }
}

