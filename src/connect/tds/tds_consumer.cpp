#include <kspp/connect/tds/tds_consumer.h>
#include <kspp/kspp.h>
#include <chrono>
#include <memory>
#include <fstream>
#include <glog/logging.h>
#include <boost/bind.hpp>
#include  <kspp/connect/tds/tds_avro_utils.h>

using namespace std::chrono_literals;

namespace kspp {
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
      if (!record.fieldAt(i).isUnion()) // TODO this should not hold - but we fail to create correct schemas for not null columns
      {
        LOG(FATAL) << "unexpected schema - bailing out, type:" << record.fieldAt(i).type();
        break;
      }
    }

    for (int dst_column = 0; dst_column < nFields; dst_column++) {
      auto src_column = tds::find_column_by_name(stream, record.schema()->nameAt(dst_column));
      if (src_column < 0)
        LOG(FATAL) << "cannot find column, name: " << record.schema()->nameAt(dst_column);

      avro::GenericDatum& col = record.fieldAt(dst_column); // expected union
      //avro::GenericUnion &au(record.fieldAt(dst_column).value<avro::GenericUnion>());

      if (dbdata(stream, src_column + 1) == nullptr) {
        col.selectBranch(0); // NULL branch - we hope..
        assert(col.type() == avro::AVRO_NULL);
        continue;
      }

      // is this a patched column - ie timeanddate2
      COL *pcol = &columns[src_column];
      if (pcol->buffer) {
        if (pcol->status == -1) {
          col.selectBranch(0); // NULL branch - we hope..
          assert(col.type() == avro::AVRO_NULL);
        } else {
          col.selectBranch(1);
          col.value<std::string>() = pcol->buffer;
        }
        continue;
      }

      // normal parsing and not null
      col.selectBranch(1);

      switch (dbcoltype(stream, src_column + 1)){
        case tds::SYBINT2: {
          int16_t v;
          assert(sizeof(v) == dbdatlen(stream, src_column + 1));
          memcpy(&v, dbdata(stream, src_column + 1), sizeof(v));
          col.value<int32_t>() = v;
        }
          break;

        case tds::SYBINT4: {
          int32_t v;
          assert(sizeof(v) == dbdatlen(stream, src_column + 1));
          memcpy(&v, dbdata(stream, src_column + 1), sizeof(v));
          col.value<int32_t>() = v;
        }
          break;

        case tds::SYBINT8: {
          int64_t v;
          assert(sizeof(v) == dbdatlen(stream, src_column + 1));
          memcpy(&v, dbdata(stream, src_column + 1), sizeof(v));
          col.value<int64_t>() = v;
        }
          break;

        case tds::SYBFLT8: {
          double v;
          assert(sizeof(v) == dbdatlen(stream, src_column + 1));
          memcpy(&v, dbdata(stream, src_column + 1), sizeof(v));
          col.value<double>() = v;
        }
          break;

        case tds::SYBCHAR: {
          std::string s;
          int sz = dbdatlen(stream, src_column + 1);
          s.reserve(sz);
          s.assign((const char *) dbdata(stream, src_column + 1), sz);
          col.value<std::string>() = s;
        }
          break;

        case tds::SYBMSUDT: {
          //std::string s;
          //int sz = dbdatlen(stream, i + 1);
          //s.reserve(sz);
          //s.assign((const char *) dbdata(stream, i + 1), sz);
          //avro_item.value<std::string>() = s;
          col.value<std::string>() = "cannot parse, type:" + std::to_string(dbcoltype(stream, src_column + 1));
        }
          break;

        case tds::SYBUNIQUE: {
          boost::uuids::uuid u;
          assert(16 == dbdatlen(stream, src_column + 1));
          memcpy(&u, dbdata(stream, src_column + 1), 16);
          // this is the same swap as you do beteen java & .net binary representation
          // I dont know which is big or little endian - but a swap is good.
          boost::uuids::uuid swapped = u;
          swapped.data[0] = u.data[3];
          swapped.data[1] = u.data[2];
          swapped.data[2] = u.data[1];
          swapped.data[3] = u.data[0];
          swapped.data[4] = u.data[5];
          swapped.data[5] = u.data[4];
          swapped.data[6] = u.data[7];
          swapped.data[7] = u.data[6];
          col.value<std::string>() = boost::uuids::to_string(swapped);
        }
          break;

        case tds::SYBBIT: {
          bool v=false;
          int sz = dbdatlen(stream, src_column + 1);
          BYTE* data = dbdata(stream, src_column + 1);

          if (*data==0)
            v = false;
          else
            v=true;

          col.value<bool>() = v;
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
          col.selectBranch(0); // NULL branch - we hope..
          assert(col.type() == avro::AVRO_NULL);
          //avro_item.value<std::string>() = "cannot_parse:" + std::to_string(dbcoltype(stream, i + 1));
          LOG(ERROR) << "unexpected / non supported type, column: " << cname << ", type:" << ctype;
        }
      } // switch sql type

    } // for dst-column
  }


  tds_consumer::tds_consumer(int32_t partition,
                             std::string logical_name,
                             const kspp::connect::connection_params& cp,
                             kspp::connect::table_params tp,
                             std::string query,
                             std::string id_column,
                             std::string ts_column,
                             std::shared_ptr<kspp::avro_schema_registry> schema_registry)
      : _bg([this] { _thread(); })
      , _connection(std::make_unique<kspp_tds::connection>())
      , _logical_name(logical_name)
      , _query(query)
      , _partition(partition)
      , _cp(cp)
      , _tp(tp)
      , _read_cursor(tp, id_column, ts_column)
      , _id_column(id_column)
      , _schema_registry(schema_registry)
      , _commit_chain(logical_name, partition)
      , _key_schema_id(-1)
      , _val_schema_id(-1)
      , _msg_cnt(0)
      , _closed(false)
      , _eof(false)
      , _start_running(false)
      , _exit(false) {
    _offset_storage = get_offset_provider(tp.offset_storage);
    std::string top_part(" TOP " + std::to_string(_tp.max_items_in_fetch));
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
    _connection.reset(nullptr);
    LOG(INFO) << "closing, read cursor at " << _read_cursor.last_tick();
  }

  void tds_consumer::close() {
    _exit = true;
    _start_running = false;

    commit(true);

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
      _connection->connect(_cp);

    // should we check more thing in database

    _start_running = true;
  }

  void tds_consumer::start(int64_t offset) {
    if (_offset_storage) {
      int64_t tmp = _offset_storage->start(offset);
      _read_cursor.set_eof(true);
      _read_cursor.start(tmp);
    } else {
      //_read_cursor will start at beginning if not initialized
    }
    initialize();
  }

  int tds_consumer::parse_row(DBPROCESS *stream, COL *columns) {
    //TODO what name should we segister this under.. source/database/table ? table seems to week

    // first time?
    if (!this->_val_schema) {

      _read_cursor.init(stream);

      auto ncols = dbnumcols(stream);
      if (!_key_schema) {
        if (_id_column.size() == 0)
          _key_schema = std::make_shared<avro::ValidSchema>(avro::NullSchema());
        else
          _key_schema = tds::schema_for_table_key(_logical_name + "_key", {_id_column}, stream);


        if (_schema_registry) {
          _key_schema_id = _schema_registry->put_schema(_logical_name + "-key", _key_schema);
        }

        std::stringstream ss0;
        _key_schema->toJson(ss0);
        LOG(INFO) << "key_schema: \n" << ss0.str();
      }

      this->_val_schema = tds::schema_for_table_row(_logical_name + "_value", stream);
      if (_schema_registry) {
        _val_schema_id = _schema_registry->put_schema(_logical_name + "-value", _val_schema); // we should probably prepend the name with a prefix (like _my_db_table_name)
      }

      // print schema first time...
      std::stringstream ss;
      this->_val_schema->toJson(ss);
      LOG(INFO) << "schema:";
      LOG(INFO) << ss.str();
    }

    // this requires the avro and the result set must have the fields in the same order
    // since we create the schema on first read this should hold
    // it could be stronger if we generated the select from schema instead of relying on select *
    // for now this should be ok if the schema is not changed between queries...

    auto key = std::make_shared<kspp::generic_avro>(_key_schema, _key_schema_id);
    load_avro_by_name(key.get(), stream, columns);
    auto val = std::make_shared<kspp::generic_avro>(_val_schema, _val_schema_id);
    load_avro_by_name(val.get(), stream, columns);

    // could be done from the shared_ptr key?
    if (!_last_key)
      _last_key = std::make_unique<kspp::generic_avro>(_key_schema, _key_schema_id);
    load_avro_by_name(_last_key.get(), stream, columns);

    _read_cursor.parse(stream);
    int64_t tick_ms = _read_cursor.last_ts_ms();
    if (tick_ms==0)
      tick_ms = kspp::milliseconds_since_epoch();

    auto record = std::make_shared<krecord<kspp::generic_avro, kspp::generic_avro>>(*key, val, tick_ms);
    // do we have one...
    int64_t tick = _read_cursor.last_tick();
    auto e = std::make_shared<kevent<kspp::generic_avro, kspp::generic_avro>>(record, tick  > 0 ? _commit_chain.create(tick) : nullptr);
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
        if (!_connection->connect(_cp))
        {
          std::this_thread::sleep_for(10s);
          continue;
        }
      }

      std::string statement = _query + _read_cursor.get_where_clause();
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

      commit(false);

      if (messages_in_batch<_tp.max_items_in_fetch) {
        _eof = true;
        _read_cursor.set_eof(_eof);
        int count = _tp.poll_intervall.count();
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
        _read_cursor.set_eof(_eof);
      }
    }
    DLOG(INFO) << "exiting thread";
  }
}
