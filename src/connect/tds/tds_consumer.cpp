#include <kspp/connect/tds/tds_consumer.h>
#include <kspp/kspp.h>
#include <chrono>
#include <memory>
#include <glog/logging.h>
#include <boost/bind.hpp>
#include  <kspp/connect/tds/tds_avro_utils.h>

using namespace std::chrono_literals;

namespace kspp {

  static std::string __order_by(const std::string& ts_column, const std::string& id_column){
    if (ts_column.size()>0 && id_column.size()>0)
      return " ORDER BY " + ts_column + " ASC, " + id_column + " ASC";
    else if (ts_column.size()>0)
      return " ORDER BY " + ts_column + " ASC ";
    else if (id_column.size()>0)
      return " ORDER BY " + id_column + " ASC ";
    return "";
  }

  tds_read_cursor::tds_read_cursor(kspp::connect::table_params tp,  std::string id_column, std::string ts_column)
  : tp_(tp)
  , _id_column(id_column)
  , _ts_column(ts_column)
  , _order_by(__order_by(_ts_column, _id_column))
  , _eof(false)
  , ts_column_index_(-1)
  , id_column_index_(-1)
  , last_ts_(INT_MIN)
  , last_id_(INT_MIN){
  }

  void tds_read_cursor::init(DBPROCESS *stream){
    if (_id_column.size()){
      std::string simple_name = tds::simple_column_name(_id_column);
      int ix = tds::find_column_by_name(stream, simple_name);
      if (ix >= 0)
        id_column_index_ = ix + 1;
      else
        LOG(FATAL) << "could not find id column: " << _id_column;
    }

    if (_ts_column.size()){
      std::string simple_name = tds::simple_column_name(_ts_column);
      int ix = tds::find_column_by_name(stream, simple_name);
      if (ix>=0)
        ts_column_index_ = ix + 1;
       else
        LOG(FATAL) << "could not find ts column: " << _ts_column;
    }
  }

  void tds_read_cursor::parse(DBPROCESS *stream) {
    if (ts_column_index_>0)
      last_ts_ = parse_ts(stream);
    if (id_column_index_>0)
      last_id_ = parse_id(stream);
  }

  int64_t tds_read_cursor::parse_ts(DBPROCESS *stream){
    assert(ts_column_index_>0);

    switch (dbcoltype(stream, ts_column_index_)){
      /*
       * case tds::SYBINT2: {
        int16_t v;
        assert(sizeof(v) == dbdatlen(stream, ts_column_index_));
        memcpy(&v, dbdata(stream, i + 1), sizeof(v));
        avro_item.value<int32_t>() = v;
      }
        break;
      */

      case tds::SYBINT4: {
        int32_t v;
        assert(sizeof(v) == dbdatlen(stream, ts_column_index_));
        memcpy(&v, dbdata(stream, ts_column_index_), sizeof(v));
        return v;
      }
        break;

      case tds::SYBINT8: {
        int64_t v;
        assert(sizeof(v) == dbdatlen(stream, ts_column_index_));
        memcpy(&v, dbdata(stream, ts_column_index_), sizeof(v));
        return v;
      }
        break;

        /*
         * case SYBDATETIME:
        case SYBDATETIME4:
        case SYBMSTIME:
        case SYBMSDATE:
        case SYBMSDATETIMEOFFSET:
        case SYBTIME:
        case SYBDATE:
        case SYB5BIGTIME:
        case SYB5BIGDATETIME:
         */
/*
        case tds::SYBMSDATETIME2: {
        std::string s;
        */
/*
         * int sz = dbdatlen(stream, i + 1);
         * s.reserve(sz);
         * s.assign((const char *) dbdata(stream, i + 1), sz);
         *//*

        s = pcol->buffer;
        avro_item.value<std::string>() = s;
      }

      break;
*/

      default:
        LOG(FATAL) << "unexpected / non supported timestamp type e:" << dbcoltype(stream, ts_column_index_);
    }
  }

  int64_t tds_read_cursor::parse_id(DBPROCESS *stream){
    assert(id_column_index_>0);

    switch (dbcoltype(stream, id_column_index_)){
      /*
       * case tds::SYBINT2: {
        int16_t v;
        assert(sizeof(v) == dbdatlen(stream, ts_column_index_));
        memcpy(&v, dbdata(stream, i + 1), sizeof(v));
        avro_item.value<int32_t>() = v;
      }
        break;
      */

      case tds::SYBINT4: {
        int32_t v;
        assert(sizeof(v) == dbdatlen(stream, id_column_index_));
        memcpy(&v, dbdata(stream, id_column_index_), sizeof(v));
        return v;
      }
        break;

      case tds::SYBINT8: {
        int64_t v;
        assert(sizeof(v) == dbdatlen(stream, id_column_index_));
        memcpy(&v, dbdata(stream, id_column_index_), sizeof(v));
        return v;
      }
        break;

        /*
         * case SYBDATETIME:
        case SYBDATETIME4:
        case SYBMSTIME:
        case SYBMSDATE:
        case SYBMSDATETIMEOFFSET:
        case SYBTIME:
        case SYBDATE:
        case SYB5BIGTIME:
        case SYB5BIGDATETIME:
         */
/*
        case tds::SYBMSDATETIME2: {
        std::string s;
        */
/*
         * int sz = dbdatlen(stream, i + 1);
         * s.reserve(sz);
         * s.assign((const char *) dbdata(stream, i + 1), sz);
         *//*

        s = pcol->buffer;
        avro_item.value<std::string>() = s;
      }

      break;
*/

      default:
        LOG(FATAL) << "unexpected / non supported id type e:" << dbcoltype(stream, id_column_index_);
    }
  }


  std::string tds_read_cursor::get_where_clause() const {
    // if we are rescraping is active at eof - and then we do not care for id at all.
    // rescrape 0 also handles when we do not have an id field
    if (_eof && last_ts_> INT_MIN && tp_.rescrape_policy == kspp::connect::LAST_QUERY_TS)
      return  " WHERE " + _ts_column + " >= '" + std::to_string(last_ts_ - tp_.rescrape_ticks) + "' " +_order_by;

    // no data in either id or ts fields?
    if (last_id_ == INT_MIN || last_ts_ == INT_MIN) {
      if (last_id_ > INT_MIN)
        return " WHERE " + _id_column + " > '" + std::to_string(last_id_) + "' " + _order_by;
      else if (last_ts_ > INT_MIN)
        return " WHERE " + _ts_column + " >= '" + std::to_string(last_ts_) + "' " + _order_by;
      else
        return "" + _order_by;
    } else {
      return " WHERE (" + _ts_column + " = '" + std::to_string(last_ts_) + "' AND " + _id_column + " > '" + std::to_string(last_id_) + "') OR (" + _ts_column + " > '" + std::to_string(last_ts_) + "') " + _order_by;
    }
  }

void tds_consumer::load_avro_by_name(kspp::generic_avro* avro, DBPROCESS *stream, COL *columns){
    // key type is null if there is no key
    if (avro->type() == avro::AVRO_NULL)
      return;

    assert(avro->type() == avro::AVRO_RECORD);
    avro::GenericRecord &record(avro->generic_datum()->value<avro::GenericRecord>());
    size_t nFields = record.fieldCount();

    // this checks the requested fields - should only be done once
    for (int i = 0; i < nFields; i++)
    {
      if (record.fieldAt(i).type() != avro::AVRO_UNION) // TODO this should not hold - but we fail to create correct schemas for not null columns
      {
        LOG(FATAL) << "unexpected schema - bailing out, type:" << record.fieldAt(i).type();
        break;
      }
    }

    for (int dst_column = 0; dst_column < nFields; dst_column++) {
      auto src_column = tds::find_column_by_name(stream, record.schema()->nameAt(dst_column));
      if (src_column < 0)
        LOG(FATAL) << "cannot find column, name: " << record.schema()->nameAt(dst_column);

      avro::GenericUnion &au(record.fieldAt(dst_column).value<avro::GenericUnion>());

      if (dbdata(stream, src_column + 1) == nullptr) {
        au.selectBranch(0); // NULL branch - we hope..
        assert(au.datum().type() == avro::AVRO_NULL);
        continue;
      }

      // is this a patched column - ie timeanddate2
      COL *pcol = &columns[src_column];
      if (pcol->buffer) {
        if (pcol->status == -1) {
          au.selectBranch(0); // NULL branch - we hope..
          assert(au.datum().type() == avro::AVRO_NULL);
        } else {
          au.selectBranch(1);
          avro::GenericDatum &avro_item(au.datum());
          std::string s = pcol->buffer;
          avro_item.value<std::string>() = s;
        }
        continue;
      }

      // normal parsing and not null
      au.selectBranch(1);
      avro::GenericDatum &avro_item(au.datum());

      switch (dbcoltype(stream, src_column + 1)){
        case tds::SYBINT2: {
          int16_t v;
          assert(sizeof(v) == dbdatlen(stream, src_column + 1));
          memcpy(&v, dbdata(stream, src_column + 1), sizeof(v));
          avro_item.value<int32_t>() = v;
        }
          break;

        case tds::SYBINT4: {
          int32_t v;
          assert(sizeof(v) == dbdatlen(stream, src_column + 1));
          memcpy(&v, dbdata(stream, src_column + 1), sizeof(v));
          avro_item.value<int32_t>() = v;
        }
          break;

        case tds::SYBINT8: {
          int64_t v;
          assert(sizeof(v) == dbdatlen(stream, src_column + 1));
          memcpy(&v, dbdata(stream, src_column + 1), sizeof(v));
          if (v==37460704755722)
            LOG(INFO) << "opps";
          avro_item.value<int64_t>() = v;
        }
          break;

        case tds::SYBFLT8: {
          double v;
          assert(sizeof(v) == dbdatlen(stream, src_column + 1));
          memcpy(&v, dbdata(stream, src_column + 1), sizeof(v));
          avro_item.value<double>() = v;
        }
          break;

        case tds::SYBCHAR: {
          std::string s;
          int sz = dbdatlen(stream, src_column + 1);
          s.reserve(sz);
          s.assign((const char *) dbdata(stream, src_column + 1), sz);
          avro_item.value<std::string>() = s;
        }
          break;

        case tds::SYBMSUDT: {
          //std::string s;
          //int sz = dbdatlen(stream, i + 1);
          //s.reserve(sz);
          //s.assign((const char *) dbdata(stream, i + 1), sz);
          //avro_item.value<std::string>() = s;
          avro_item.value<std::string>() = "cannot parse, type:" + std::to_string(dbcoltype(stream, src_column + 1));
        }
          break;

        case tds::SYBUNIQUE:
          avro_item.value<std::string>() = "cannot parse, type: " + std::to_string(dbcoltype(stream, src_column + 1));
          break;

        case tds::SYBBIT: {
          bool v=false;
          int sz = dbdatlen(stream, src_column + 1);
          BYTE* data = dbdata(stream, src_column + 1);

          if (*data==0)
            v = false;
          else
            v=true;

          avro_item.value<bool>() = v;
        }
          break;


          /*
           * case SYBDATETIME:
          case SYBDATETIME4:
          case SYBMSTIME:
          case SYBMSDATE:
          case SYBMSDATETIMEOFFSET:
          case SYBTIME:
          case SYBDATE:
          case SYB5BIGTIME:
          case SYB5BIGDATETIME:
           */
        case tds::SYBMSDATETIME2: {
          //std::string s;
          /*
           * int sz = dbdatlen(stream, i + 1);
           * s.reserve(sz);
           * s.assign((const char *) dbdata(stream, i + 1), sz);
           */
          //s = pcol->buffer;
          //avro_item.value<std::string>() = s;
        }
          break;

        default:{
          const char* cname = dbcolname(stream, src_column + 1);
          int ctype = dbcoltype(stream, src_column + 1);
          au.selectBranch(0); // NULL branch - we hope..
          assert(au.datum().type() == avro::AVRO_NULL);
          //avro_item.value<std::string>() = "cannot_parse:" + std::to_string(dbcoltype(stream, i + 1));
          LOG(ERROR) << "unexpected / non supported type, column: " << cname << ", type:" << ctype;
        }
      } // switch sql type

    } // for dst-column
  }


  tds_consumer::tds_consumer(int32_t partition,
                             std::string logical_name,
                             std::string consumer_group,
                             const kspp::connect::connection_params& cp,
                             kspp::connect::table_params tp,
                             std::string query,
                             std::string id_column,
                             std::string ts_column,
                             std::shared_ptr<kspp::avro_schema_registry> schema_registry)
      : _bg([this] { _thread(); })
      , _connection(std::make_shared<kspp_tds::connection>())
      , _logical_name(logical_name)
      , _query(query)
      , _partition(partition)
      , _consumer_group(consumer_group)
      , cp_(cp)
      , tp_(tp)
      , _read_cursor(tp, id_column, ts_column)
      , _id_column(id_column)
      //, _ts_column(ts_column)
      //, id_column_index_(0)
      //, ts_column_index_(0)
      //, last_ts_(INT_MIN)
      //, last_id_(INT_MIN)
      , schema_registry_(schema_registry)
      , key_schema_id_(-1)
      , val_schema_id_(-1)
      , _msg_cnt(0)
      , _good(true)
      , _closed(false)
      , _eof(false)
      , _start_running(false)
      , _exit(false) {
    std::string top_part(" TOP " + std::to_string(tp_.max_items_in_fetch));
    // assumed to start with "SELECT"
    _query.insert(6,top_part);
    LOG(INFO) << " REAL QUERY: "  << _query;
  }

  tds_consumer::~tds_consumer() {
    _exit = true;
    if (!_closed)
      close();
    _bg.join();
    _connection->close();
    _connection = nullptr;
  }

  void tds_consumer::close() {
    _exit = true;
    _start_running = false;

    if (_closed)
      return;
    _closed = true;

    if (_connection) {
      _connection->close();
      LOG(INFO) << "tds_consumer table:" << _logical_name << ":" << _partition << ", closed - consumed " << _msg_cnt << " messages";
    }
  }

  bool tds_consumer::initialize() {
    if (!_connection->connected())
      _connection->connect(cp_);

    // should we check more thing in database

    _start_running = true;
  }

  void tds_consumer::start(int64_t offset) {
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
      DLOG(INFO) << "tds_consumer::start table:" << _logical_name << ":" << _partition << " consumer group: "
                 << _consumer_group << " starting from OFFSET_BEGINNING";
    } else if (offset == kspp::OFFSET_END) {
      DLOG(INFO) << "tds_consumer::start table:" << _logical_name << ":" << _partition << " consumer group: "
                 << _consumer_group << " starting from OFFSET_END";
    } else {
      DLOG(INFO) << "tds_consumer::start table:" << _logical_name << ":" << _partition << " consumer group: "
                 << _consumer_group << " starting from fixed offset: " << offset;
    }

    initialize();
  }

  /*
  int32_t tds_consumer::commit(int64_t offset, bool flush) {
  }
   */




  int tds_consumer::parse_row(DBPROCESS *stream, COL *columns) {
    //TODO what name should we segister this under.. source/database/table ? table seems to week

    // first time?
    if (!this->val_schema_) {

      _read_cursor.init(stream);

      auto ncols = dbnumcols(stream);
      if (!key_schema_) {
        if (_id_column.size() == 0)
          key_schema_ = std::make_shared<avro::ValidSchema>(avro::NullSchema());
         else
          key_schema_ = tds::schema_for_table_key(_logical_name + "_key", {_id_column}, stream);


        if (schema_registry_) {
          key_schema_id_ = schema_registry_->put_schema(_logical_name + "-key", key_schema_);
        }

        std::stringstream ss0;
        key_schema_->toJson(ss0);
        LOG(INFO) << "key_schema: \n" << ss0.str();
      }

      this->val_schema_ = tds::schema_for_table_row(_logical_name + "_value", stream);
      if (schema_registry_) {
        val_schema_id_ = schema_registry_->put_schema(_logical_name + "-value", val_schema_); // we should probably prepend the name with a prefix (like _my_db_table_name)
      }

      // print schema first time...
      std::stringstream ss;
      this->val_schema_->toJson(ss);
      LOG(INFO) << "schema:";
      LOG(INFO) << ss.str();
    }

    // this requires the avro and the result set must have the fields in the same order
    // since we create the schema on first read this should hold
    // it could be stronger if we generated the select from schema instead of relying on select *
    // for now this should be ok if the schema is not changed between queries...

    auto key = std::make_shared<kspp::generic_avro>(key_schema_, key_schema_id_);
    load_avro_by_name(key.get(), stream, columns);
    auto val = std::make_shared<kspp::generic_avro>(val_schema_, val_schema_id_);
    load_avro_by_name(val.get(), stream, columns);

    // could be done from the shared_ptr key?
    if (!last_key_)
      last_key_ = std::make_unique<kspp::generic_avro>(key_schema_, key_schema_id_);
    load_avro_by_name(last_key_.get(), stream, columns);

    _read_cursor.parse(stream);

    auto record = std::make_shared<krecord<kspp::generic_avro, kspp::generic_avro>>(*key, val, kspp::milliseconds_since_epoch());
    auto e = std::make_shared<kevent<kspp::generic_avro, kspp::generic_avro>>(record);
    assert(e.get()!=nullptr);

    ++_msg_cnt;

    while(_incomming_msg.size()>10000 && !_exit) {
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
      DLOG(INFO) << "c_incomming_msg.size() " << _incomming_msg.size();
    }

    _incomming_msg.push_back(e);
  }

  int tds_consumer::parse_response(DBPROCESS *stream) {
    if (dbhasretstat(stream) == TRUE) {
      DLOG(INFO) << "Procedure returned: " << dbretstatus(stream);
    } else {
      DLOG(INFO) << "Procedure returned: none";
    }

    //int ncomputeids = dbnumcompute(stream);

    RETCODE erc;
    while ((erc = dbresults(stream)) != NO_MORE_RESULTS) {
      if (erc == FAIL) {
        LOG(FATAL) << "dbresults failed";
      }

      auto ncols = dbnumcols(stream);

      /*
       * Read metadata and bind.
       * the only reason we do this is to parse SYBMSDATETIME2 (which we should lear how to do)
       */
      COL *columns = nullptr;

      if ((columns = (COL *) calloc(ncols, sizeof(struct COL))) == NULL) {
        perror(NULL);
        exit(1);
      }

      for (COL *pcol = columns; pcol - columns < ncols; pcol++) {
        int c = pcol - columns + 1;

        pcol->name = dbcolname(stream, c);
        pcol->type = dbcoltype(stream, c);
        pcol->size = 0; // AUTOCONV SIZE

        if (pcol->type == SYBMSDATETIME2) {
          pcol->size = 64; //test
        }

        //if (pcol->type==SYBMSUDT) {
        //  pcol->size = 64; //test
        //}

        if (pcol->size > 0) {

          if ((pcol->buffer = (char *) calloc(1, pcol->size + 1)) == NULL) {
            perror(NULL);
            exit(1);
          }

          auto erc = dbbind(stream, c, NTBSTRINGBIND,
                            pcol->size + 1, (BYTE *) pcol->buffer);
          if (erc == FAIL) {
            LOG(FATAL) << "dbbind( " << c << ") failed";
          }

          erc = dbnullbind(stream, c, &pcol->status);
          if (erc == FAIL) {
            LOG(FATAL) << "dbnullbind( " << c << ") failed";
          }
        }

      }

      int row_code = 0;
      while ((row_code = dbnextrow(stream)) != NO_MORE_ROWS) {
        switch (row_code) {
          case REG_ROW:
            parse_row(stream, columns);
            break;

          case BUF_FULL:
            assert(false);
            break;

          case FAIL:
            LOG(FATAL) << "dbresults failed";
            break;

          default:
            LOG(INFO) << "Data for computeid " << row_code << " ignored";
        }
      }

      DLOG(INFO) << "deleting ";
      for (COL *pcol = columns; pcol - columns < ncols; pcol++) {
        if (pcol->buffer)
          free(pcol->buffer);
      }
      free(columns);
    }
  }


  void tds_consumer::_thread() {
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
        //_eof = false;
        if (!_connection->connect(cp_))
        {
          std::this_thread::sleep_for(10s);
          continue;
        }
      }

      /*
       * std::string fields = "*";
      std::string order_by = "";
      if (_ts_column.size())
        order_by = _ts_column + " ASC, " + _id_column + " ASC";
      else
        order_by = _id_column + " ASC";
      */


      std::string where_clause =  _read_cursor.get_where_clause();

/*      // do we have a timestamp field
      // we have to have either a interger id that is increasing or a timestamp that is increasing
      // before we read anything the where clause will not be valid
      if (last_id_>INT_MIN) {
        if (_ts_column.size()) {
          switch (tp_.rescrape_policy) {
            case kspp::connect::RESCRAPE_OFF: {
              if (tp_.row_constness != kspp::connect::IMMUTABLE && _eof){
                where_clause = " WHERE " + _ts_column + " >= '" + std::to_string(last_ts_) + "'";
              } else {
                where_clause = " WHERE (" + _ts_column + " = '" + std::to_string(last_ts_) + "' AND " + _id_column + " > '" + std::to_string(last_id_) + "') OR (" + _ts_column + " > '" + std::to_string(last_ts_) + "')";
              }
            }
            break;

            case kspp::connect::LAST_QUERY_TS:
              if (_eof) {
                where_clause = " WHERE " + _ts_column + " >= '" + std::to_string(last_ts_ - tp_.rescrape_ticks) + "'";
              } else {
                where_clause = " WHERE (" + _ts_column + " = '" + std::to_string(last_ts_) + "' AND " + _id_column + " > '" + std::to_string(last_id_) + "') OR (" +  _ts_column + " > '" + std::to_string(last_ts_) + "')";
              }
              break;

            *//* we have to have a generic way of trasforming client ts to server ts...
             * case kspp::connect::CLIENT_TS:
              if (_eof) {
                where_clause = " WHERE " + _ts_column + " >= '" + std::to_string(last_ts_ - tp_.rescrape_ticks) + "'";
              } else {
                where_clause =
                    " WHERE (" + _ts_column + " = '" + std::to_string(last_ts_) + "' AND " + _id_column + " > '" +
                    std::to_string(last_id_) + "') OR (" +
                    _ts_column + " > '" + std::to_string(last_ts_) + "')";
              }
              break;
            *//*

            default:
              LOG(FATAL) << "bad - rescrape_policy: " << tp_.rescrape_policy;
          }
        } else {
          where_clause = " WHERE " + _id_column + " > '" + std::to_string(last_id_) + "'";
        }
      } else {
        if (last_ts_> INT_MIN)
          if (tp_.connect_ts_policy == kspp::connect::GREATER) // this leads to potential data loss
            where_clause = " WHERE " + _ts_column + " > '" + std::to_string(last_ts_) + "'";
          else if (tp_.connect_ts_policy == kspp::connect::GREATER_OR_EQUAL) // this leads to duplicates since last is repeated
            where_clause = " WHERE " + _ts_column + " >= '" + std::to_string(last_ts_) + "'";
      }*/

      //LOG(INFO) << _logical_name << " - WHERE_CLAUSE... " << where_clause;

      /*
      // do we have a timestamp field
      // we have to have either a interger id that is increasing or a timestamp that is increasing
     if (_ts_column.size())
          where_clause = " WHERE " + _ts_column + " >= " + std::to_string(last_ts_);
        //else
        //  where_clause = _id_column + " >= " + std::to_string(last_id_);
        */

      //std::string statement = "SELECT TOP " + std::to_string(_max_items_in_fetch) + " " + fields + " FROM " + _table + where_clause + order_by;
      std::string statement = _query + where_clause;
      DLOG(INFO) << "exec(" + statement + ")";


      auto ts0 = kspp::milliseconds_since_epoch();
      auto last_msg_count = _msg_cnt;
      auto res = _connection->exec(statement);
      if (res.first) {
        LOG(ERROR) << "exec failed - disconnecting and retrying";
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



      size_t messages_in_batch = _msg_cnt - last_msg_count;

      if (messages_in_batch==0) {
        LOG_EVERY_N(INFO, 100) << "empty poll done, table: " << _logical_name << " total: " << _msg_cnt << ", last ts: "
                               << _read_cursor.last_ts() << " duration " << ts1 - ts0 << " ms";
      }
      else {
        LOG(INFO) << "poll done, table: " << _logical_name << " retrieved: " << messages_in_batch
                  << " messages, total: " << _msg_cnt << ", last ts: " << _read_cursor.last_ts() << " duration " << ts1 - ts0
                  << " ms";
      }

      if (messages_in_batch<tp_.max_items_in_fetch) {
        _eof = true;
        int count = tp_.poll_intervall.count();
        if (count>61) {
          LOG(INFO) << "sleeping POLL INTERVALL: " << count;
        }
        for (int i = 0; i != count; ++i) {
          std::this_thread::sleep_for(1s);
          if (_exit)
            break;
        }
      } else {
        _eof = false;
      }
    }
    DLOG(INFO) << "exiting thread";
  }
}

