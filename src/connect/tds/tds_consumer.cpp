#include <kspp/connect/tds/tds_consumer.h>
#include <kspp/kspp.h>
#include <chrono>
#include <memory>
#include <glog/logging.h>
#include <boost/bind.hpp>
#include  <kspp/connect/tds/tds_avro_utils.h>

using namespace std::chrono_literals;

namespace kspp {
  tds_consumer::tds_consumer(int32_t partition,
                             std::string table,
                             std::string consumer_group,
                             const kspp::connect::connection_params& cp,
                             std::string id_column,
                             std::string ts_column,
                             std::shared_ptr<kspp::avro_schema_registry> schema_registry,
                             std::chrono::seconds poll_intervall,
                             size_t max_items_in_fetch)
      : _bg([this] { _thread(); })
      , _connection(std::make_shared<kspp_tds::connection>())
      , _table(table)
      , _partition(partition)
      , _consumer_group(consumer_group)
      , cp_(cp)
      , _id_column(id_column)
      , _ts_column(ts_column)
      , id_column_index_(0)
      , ts_column_index_(0)
      , last_ts_(INT_MIN)
      , last_id_(INT_MIN)
      , schema_registry_(schema_registry)
      , _max_items_in_fetch(max_items_in_fetch)
      , schema_id_(-1)
      , poll_intervall_(poll_intervall)
      , _msg_cnt(0)
      , _good(true)
      , _closed(false)
      , _eof(false)
      , _start_running(false)
      , _exit(false) {
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
      LOG(INFO) << "tds_consumer table:" << _table << ":" << _partition << ", closed - consumed " << _msg_cnt << " messages";
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
      DLOG(INFO) << "tds_consumer::start table:" << _table << ":" << _partition << " consumer group: "
                 << _consumer_group << " starting from OFFSET_BEGINNING";
    } else if (offset == kspp::OFFSET_END) {
      DLOG(INFO) << "tds_consumer::start table:" << _table << ":" << _partition << " consumer group: "
                 << _consumer_group << " starting from OFFSET_END";
    } else {
      DLOG(INFO) << "tds_consumer::start table:" << _table << ":" << _partition << " consumer group: "
                 << _consumer_group << " starting from fixed offset: " << offset;
    }

    initialize();
  }

  /*
  int32_t tds_consumer::commit(int64_t offset, bool flush) {
  }
   */

  int64_t tds_consumer::parse_ts(DBPROCESS *stream){
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

  int64_t tds_consumer::parse_id(DBPROCESS *stream){
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


  int tds_consumer::parse_avro(DBPROCESS *stream, COL *columns, size_t ncols) {
    //TODO what name should we segister this under.. source/database/table ? table seems to week


    // first time?
    if (!this->schema_) {
      //std::string schema_name =  host_ + "_" +  database_ + "_" + _table;
      std::string schema_name = "tds_" + cp_.database + "_" + _table;
      this->schema_ = std::make_shared<avro::ValidSchema>(*tds::schema_for_table_row(schema_name, stream));
      if (schema_registry_) {
        schema_id_ = schema_registry_->put_schema(schema_name, schema_); // we should probably prepend the name with a prefix (like _my_db_table_name)
      }

      // find ts field
      if (_ts_column.size()){
        for (int i = 0; i < ncols; i++) {
          const char *col_name = dbcolname(stream, i + 1);
          if (strcmp(col_name, _ts_column.c_str()) == 0) {
            ts_column_index_ = i + 1;
          }
        }
        if (ts_column_index_<1){
          LOG(FATAL) << "could not find ts column: " <<  _ts_column;
        }
      }

      if (_id_column.size()){
        for (int i = 0; i < ncols; i++) {
          const char *col_name = dbcolname(stream, i + 1);
          if (strcmp(col_name, _ts_column.c_str()) == 0) {
            id_column_index_ = i + 1;
          }
        }
        if (ts_column_index_<1){
          LOG(FATAL) << "could not find id column: " <<  _id_column;
        }
      }

      // print schema first time...
      std::stringstream ss;
      this->schema_->toJson(ss);
      LOG(INFO) << "schema:";
      LOG(INFO) << ss.str();
    }

    // this requires the avro and the result set must have the fields in the same order
    // since we create the schema on first read this should hold
    // it could be stronger if we generated the select from schema instead of relying on select *
    // for now this should be ok if the schema is not changed between queries...
    auto gd = std::make_shared<kspp::generic_avro>(schema_, schema_id_);
    assert(gd->type() == avro::AVRO_RECORD);
    avro::GenericRecord &record(gd->generic_datum()->value<avro::GenericRecord>());
    assert(ncols = record.fieldCount());

    COL *pcol = columns;

    if (ts_column_index_>0) {
      last_ts_ = parse_ts(stream);
    }

    if (id_column_index_>0){
      last_id_ = parse_id(stream);
    }

    for (int i = 0; i != ncols; i++, pcol++) {
      if (record.fieldAt(i).type() != avro::AVRO_UNION) {
        LOG(ERROR) << "unexpected schema - bailing out, type:" << record.fieldAt(i).type();
        assert(false);
        break;
      }
      avro::GenericUnion &au(record.fieldAt(i).value<avro::GenericUnion>());

      bool is_null = false;
      bool is_patched = false;
      // check if null - the pcol->buffer is for the patched conversion thing we dont know how to convert ourselves
      if (pcol->buffer) {
        is_patched = true;
        if (pcol->status == -1)
          is_null = true;
      } else {
        if (dbdata(stream, i + 1) == nullptr)
          is_null = true;
      }

      /*
        const char *buffer = (pcol->status == -1) ? "NULL" : pcol->buffer;
        std::cerr << dbcolname(stream, i + 1) << ": " << buffer << std::endl;

      } else {
        print_data(dbcolname(stream, i + 1), dbcoltype(stream, i + 1), dbdata(stream, i + 1),
                   dbdatlen(stream, i + 1));
      }
      */

      if (is_null) {
        au.selectBranch(0); // NULL branch - we hope..
        assert(au.datum().type() == avro::AVRO_NULL);
      } else {
        au.selectBranch(1);
        avro::GenericDatum &avro_item(au.datum());

        switch (dbcoltype(stream, i + 1)){
          case tds::SYBINT2: {
            int16_t v;
            assert(sizeof(v) == dbdatlen(stream, i + 1));
            memcpy(&v, dbdata(stream, i + 1), sizeof(v));
            avro_item.value<int32_t>() = v;
          }
            break;

          case tds::SYBINT4: {
            int32_t v;
            assert(sizeof(v) == dbdatlen(stream, i + 1));
            memcpy(&v, dbdata(stream, i + 1), sizeof(v));
            avro_item.value<int32_t>() = v;
          }
            break;

          case tds::SYBINT8: {
            int64_t v;
            assert(sizeof(v) == dbdatlen(stream, i + 1));
            memcpy(&v, dbdata(stream, i + 1), sizeof(v));
            avro_item.value<int64_t>() = v;
          }
            break;

          case tds::SYBFLT8: {
            double v;
            assert(sizeof(v) == dbdatlen(stream, i + 1));
            memcpy(&v, dbdata(stream, i + 1), sizeof(v));
            avro_item.value<double>() = v;
          }
            break;

          case tds::SYBCHAR: {
            std::string s;
            int sz = dbdatlen(stream, i + 1);
            s.reserve(sz);
            s.assign((const char *) dbdata(stream, i + 1), sz);
            avro_item.value<std::string>() = s;
          }
            break;

          case tds::SYBMSUDT: {
            //std::string s;
            //int sz = dbdatlen(stream, i + 1);
            //s.reserve(sz);
            //s.assign((const char *) dbdata(stream, i + 1), sz);
            //avro_item.value<std::string>() = s;
            avro_item.value<std::string>() = "cannot parse" + std::to_string(dbcoltype(stream, i + 1));
          }
          break;

          case tds::SYBBIT: {
            bool v=false;
            int sz = dbdatlen(stream, i + 1);
            BYTE* data = dbdata(stream, i + 1);

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
            std::string s;
            /*
             * int sz = dbdatlen(stream, i + 1);
             * s.reserve(sz);
             * s.assign((const char *) dbdata(stream, i + 1), sz);
             */
            s = pcol->buffer;
            avro_item.value<std::string>() = s;
          }
            break;

          default:{
                   int ctype = dbcoltype(stream, i + 1);
                   char* cname = dbcolname(stream, i + 1);
                   avro_item.value<std::string>() = "cannot_parse:" + std::to_string(dbcoltype(stream, i + 1));
          }
            //LOG(FATAL) << "unexpected / non supported type e:" << dbcoltype(stream, i + 1);
        }
      }
    }
    int64_t ts = 0; // TODO get the timestamp column
    auto r = std::make_shared<krecord<void, kspp::generic_avro>>(gd, ts);
    auto e = std::make_shared<kevent<void, kspp::generic_avro>>(r);
    assert(e.get() != nullptr);
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
            parse_avro(stream, columns, ncols);
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
        if (!_connection->connect(cp_))
        {
          std::this_thread::sleep_for(10s);
          continue;
        }
      }

      _eof = false;

      /*
       * std::string fields = "*";
      std::string order_by = "";
      if (_ts_column.size())
        order_by = _ts_column + " ASC, " + _id_column + " ASC";
      else
        order_by = _id_column + " ASC";
      */

      std::string fields = "*";
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
      if (last_id_>INT_MIN) {
        if (_ts_column.size())
          where_clause =
            " WHERE (" + _ts_column + " = '" + std::to_string(last_ts_) + "' AND " + _id_column + " > '" + std::to_string(last_id_) + "') OR (" +
            _ts_column + " > '" + std::to_string(last_ts_) + "')";
        else
          where_clause = " WHERE " + _id_column + " > '" + std::to_string(last_id_) + "'";
      } else {
        if (last_ts_> INT_MIN)
          if (cp_.connect_ts_policy == kspp::connect::GREATER) // this leads to potential data loss
            where_clause = " WHERE " + _ts_column + " > '" + std::to_string(last_ts_) + "'";
          else if (cp_.connect_ts_policy == kspp::connect::GREATER_OR_EQUAL) // this leads to duplicates since last is repeated
            where_clause = " WHERE " + _ts_column + " >= '" + std::to_string(last_ts_) + "'";
      }

      LOG(INFO) << "WHERE_CLAUSE " << where_clause;

      /*
      // do we have a timestamp field
      // we have to have either a interger id that is increasing or a timestamp that is increasing
     if (_ts_column.size())
          where_clause = " WHERE " + _ts_column + " >= " + std::to_string(last_ts_);
        //else
        //  where_clause = _id_column + " >= " + std::to_string(last_id_);
        */

      std::string statement = "SELECT TOP " + std::to_string(_max_items_in_fetch) + " " + fields + " FROM " + _table + where_clause + order_by;

      // TODO where ts > ....

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

      _eof = true;

      size_t messages_in_batch = _msg_cnt - last_msg_count;

      if (messages_in_batch==0) {
        LOG_EVERY_N(INFO, 100) << "empty poll done, table: " << _table << " total: " << _msg_cnt << ", last ts: "
                               << last_ts_ << " duration " << ts1 - ts0 << " ms";
      }
      else {
        LOG(INFO) << "poll done, table: " << _table << " retrieved: " << messages_in_batch
                  << " messages, total: " << _msg_cnt << ", last ts: " << last_ts_ << " duration " << ts1 - ts0
                  << " ms";
      }

      // if we got a lot of messages back run again
      if ((_msg_cnt - last_msg_count) < 10) {
        // if we sleep long we cannot be killed
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

