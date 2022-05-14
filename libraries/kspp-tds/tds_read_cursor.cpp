#include <kspp-tds/tds_read_cursor.h>
#include <kspp/kspp.h>
#include <chrono>
#include <memory>
#include <glog/logging.h>
#include <kspp-tds/tds_avro_utils.h>

using namespace std::chrono_literals;

namespace kspp {
  static std::string __order_by(const std::string &ts_column, const std::string &id_column) {
    if (ts_column.size() > 0 && id_column.size() > 0)
      return " ORDER BY " + ts_column + " ASC, " + id_column + " ASC";
    else if (ts_column.size() > 0)
      return " ORDER BY " + ts_column + " ASC ";
    else if (id_column.size() > 0)
      return " ORDER BY " + id_column + " ASC ";
    return "";
  }

  tds_read_cursor::tds_read_cursor(kspp::connect::table_params tp, std::string id_column, std::string ts_column)
      : tp_(tp)
        , id_column_(id_column)
        , ts_column_(ts_column)
        , order_by_(__order_by(ts_column_, id_column_))
        , ts_multiplier_(tp.ts_multiplier)
        , ts_utc_offset_(tp.ts_utc_offset) {
    assert(ts_multiplier_ >= 1);
  }

  void tds_read_cursor::init(DBPROCESS *stream) {
    if (id_column_.size()) {
      std::string simple_name = tds::simple_column_name(id_column_);
      int ix = tds::find_column_by_name(stream, simple_name);
      if (ix >= 0)
        id_column_index_ = ix + 1;
      else
        LOG(FATAL) << "could not find id column: " << id_column_;
    }

    if (ts_column_.size()) {
      std::string simple_name = tds::simple_column_name(ts_column_);
      int ix = tds::find_column_by_name(stream, simple_name);
      if (ix >= 0)
        ts_column_index_ = ix + 1;
      else
        LOG(FATAL) << "could not find ts column: " << ts_column_;
    }
  }

  void tds_read_cursor::start(int64_t ts) {
    if (!ts_column_.size())
      LOG(FATAL) << "start from ts but no timestamp column";

    last_ts_ticks_ = ts;
    last_ts_ = std::to_string(last_ts_ticks_);
  }

  void tds_read_cursor::parse(DBPROCESS *stream) {
    if (ts_column_index_ > 0) {
      last_ts_ticks_ = parse_ts(stream);
      last_ts_ = std::to_string(last_ts_ticks_);
    }
    if (id_column_index_ > 0)
      last_id_ = std::to_string(parse_id(stream));
  }

  int64_t tds_read_cursor::parse_ts(DBPROCESS *stream) {
    assert(ts_column_index_ > 0);

    switch (dbcoltype(stream, ts_column_index_)) {
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

  int64_t tds_read_cursor::parse_id(DBPROCESS *stream) {
    assert(id_column_index_ > 0);

    switch (dbcoltype(stream, id_column_index_)) {
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
    if (eof_ && last_ts_.size() && tp_.rescrape_policy == kspp::connect::LAST_QUERY_TS)
      return " WHERE " + ts_column_ + " >= '" + std::to_string(last_ts_ticks_ - tp_.rescrape_ticks) + "' " + order_by_;

    // no data in either id or ts fields?
    if (!last_id_.size() || !last_ts_.size()) {
      if (last_id_.size())
        return " WHERE " + id_column_ + " > '" + last_id_ + "' " + order_by_;
      else if (last_ts_.size())
        return " WHERE " + ts_column_ + " >= '" + last_ts_ + "' " + order_by_;
      else
        return "" + order_by_;
    } else {
      return " WHERE (" + ts_column_ + " = '" + last_ts_ + "' AND " + id_column_ + " > '" + last_id_ + "') OR (" +
             ts_column_ + " > '" + last_ts_ + "') " + order_by_;
    }
  }
}