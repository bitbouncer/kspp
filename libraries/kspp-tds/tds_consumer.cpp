#include <kspp-tds/tds_consumer.h>
#include <kspp/kspp.h>
#include <chrono>
#include <memory>
#include <fstream>
#include <glog/logging.h>
#include <kspp-tds/tds_avro_utils.h>

using namespace std::chrono_literals;

namespace kspp {
  void tds_consumer::load_avro_by_name(kspp::generic_avro *avro, DBPROCESS *stream, COL *columns) {
    // key type is null if there is no key
    if (avro->type() == avro::AVRO_NULL)
      return;

    assert(avro->type() == avro::AVRO_RECORD);
    avro::GenericRecord &record(avro->generic_datum()->value<avro::GenericRecord>());
    size_t nFields = record.fieldCount();

    // this checks the requested fields - should only be done once
    for (size_t i = 0; i < nFields; i++) {
      if (!record.fieldAt(
          i).isUnion()) // TODO this should not hold - but we fail to create correct schemas for not null columns
      {
        LOG(FATAL) << "unexpected schema - bailing out, type:" << record.fieldAt(i).type();
        break;
      }
    }

    for (size_t dst_column = 0; dst_column < nFields; dst_column++) {
      auto src_column = tds::find_column_by_name(stream, record.schema()->nameAt(dst_column));
      if (src_column < 0)
        LOG(FATAL) << "cannot find column, name: " << record.schema()->nameAt(dst_column);

      avro::GenericDatum &col = record.fieldAt(dst_column); // expected union
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

      switch (dbcoltype(stream, src_column + 1)) {
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
          bool v = false;
          //int sz = dbdatlen(stream, src_column + 1);
          BYTE *data = dbdata(stream, src_column + 1);

          if (*data == 0)
            v = false;
          else
            v = true;

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

        default: {
          const char *cname = dbcolname(stream, src_column + 1);
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
                             const kspp::connect::connection_params &cp,
                             kspp::connect::table_params tp,
                             std::string query,
                             std::string id_column,
                             std::string ts_column,
                             std::shared_ptr<kspp::avro_schema_registry> schema_registry)
      : connection_(std::make_unique<kspp_tds::connection>())
      , cp_(cp)
      , tp_(tp)
      , query_(query)
      , logical_name_(avro_utils::sanitize_schema_name(logical_name))
      , partition_(partition)
      , id_column_(id_column)
      , schema_registry_(schema_registry)
      , commit_chain_(logical_name, partition)
      , read_cursor_(tp, id_column, ts_column)
      , bg_([this] { _thread(); }) {
    offset_storage_ = get_offset_provider(tp.offset_storage);
    std::string top_part(" TOP " + std::to_string(tp_.max_items_in_fetch));
    // assumed to start with "SELECT"
    query_.insert(6, top_part);
    LOG(INFO) << " REAL QUERY: " << query_;
  }

  tds_consumer::~tds_consumer() {
    exit_ = true;
    if (!closed_)
      close();
    bg_.join();
    connection_->close();
    connection_.reset(nullptr);
    LOG(INFO) << "closing, read cursor at " << read_cursor_.last_tick();
  }

  void tds_consumer::close() {
    exit_ = true;
    start_running_ = false;
    commit(true);
    if (closed_)
      return;
    closed_ = true;
    if (connection_) {
      connection_->close();
      LOG(INFO) << "tds_consumer table:" << logical_name_ << ":" << partition_ << ", closed - consumed " << msg_cnt_
                << " messages";
    }
  }

  bool tds_consumer::initialize() {
    if (!connection_->connected())
      connection_->connect(cp_);
    // should we check more thing in database
    start_running_ = true;
    return true;
  }

  void tds_consumer::start(int64_t offset) {
    if (offset_storage_) {
      int64_t tmp = offset_storage_->start(offset);
      read_cursor_.set_eof(true);
      read_cursor_.start(tmp);
    } else {
      //_read_cursor will start at beginning if not initialized
    }
    initialize();
  }

  int tds_consumer::parse_row(DBPROCESS *stream, COL *columns) {
    //TODO what name should we segister this under.. source/database/table ? table seems to week

    // first time?
    if (!this->val_schema_) {

      read_cursor_.init(stream);

      //auto ncols = dbnumcols(stream);
      if (!key_schema_) {
        if (id_column_.size() == 0)
          key_schema_ = std::make_shared<avro::ValidSchema>(avro::NullSchema());
        else
          key_schema_ = tds::schema_for_table_key(logical_name_ + "_key", {id_column_}, stream);


        if (schema_registry_) {
          key_schema_id_ = schema_registry_->put_schema(logical_name_ + "-key", key_schema_);
        }

        std::stringstream ss0;
        key_schema_->toJson(ss0);
        LOG(INFO) << "key_schema: \n" << ss0.str();
      }

      this->val_schema_ = tds::schema_for_table_row(logical_name_ + "_value", stream);
      if (schema_registry_) {
        val_schema_id_ = schema_registry_->put_schema(logical_name_ + "-value",
                                                      val_schema_); // we should probably prepend the name with a prefix (like _my_db_table_name)
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

    read_cursor_.parse(stream);
    int64_t tick_ms = read_cursor_.last_ts_ms();
    if (tick_ms == 0)
      tick_ms = kspp::milliseconds_since_epoch();

    auto record = std::make_shared<krecord<kspp::generic_avro, kspp::generic_avro>>(*key, val, tick_ms);
    // do we have one...
    int64_t tick = read_cursor_.last_tick();
    auto e = std::make_shared<kevent<kspp::generic_avro, kspp::generic_avro>>(record,
                                                                              tick > 0 ? commit_chain_.create(tick)
                                                                                       : nullptr);
    assert(e.get() != nullptr);
    ++msg_cnt_;
    while (incomming_msg_.size() > 10000 && !exit_) {
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
      DLOG(INFO) << "c_incomming_msg.size() " << incomming_msg_.size();
    }
    incomming_msg_.push_back(e);
    return 0;
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
    return 0;
  }

  void tds_consumer::_thread() {
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
        //_eof = false;
        if (!connection_->connect(cp_)) {
          std::this_thread::sleep_for(10s);
          continue;
        }
      }

      std::string statement = query_ + read_cursor_.get_where_clause();
      DLOG(INFO) << "exec(" + statement + ")";

      auto ts0 = kspp::milliseconds_since_epoch();
      auto last_msg_count = msg_cnt_;
      auto res = connection_->exec(statement);
      if (res.first) {
        LOG(ERROR) << "exec failed - disconnecting and retrying";
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

      size_t messages_in_batch = msg_cnt_ - last_msg_count;

      if (messages_in_batch == 0) {
        LOG_EVERY_N(INFO, 100) << "empty poll done, table: " << logical_name_ << " total: " << msg_cnt_ << ", last ts: "
                               << read_cursor_.last_ts() << " duration " << ts1 - ts0 << " ms";
      } else {
        LOG(INFO) << "poll done, table: " << logical_name_ << " retrieved: " << messages_in_batch
                  << " messages, total: " << msg_cnt_ << ", last ts: " << read_cursor_.last_ts() << " duration "
                  << ts1 - ts0
                  << " ms";
      }

      commit(false);

      if (messages_in_batch < tp_.max_items_in_fetch) {
        eof_ = true;
        read_cursor_.set_eof(eof_);
        int count = tp_.poll_intervall.count();
        if (count > 61) {
          LOG(INFO) << "sleeping POLL INTERVALL: " << count;
        }
        for (int i = 0; i != count; ++i) {
          std::this_thread::sleep_for(1s);
          if (exit_)
            break;
        }
      } else {
        eof_ = false;
        read_cursor_.set_eof(eof_);
      }
    }
    DLOG(INFO) << "exiting thread";
  }
}
