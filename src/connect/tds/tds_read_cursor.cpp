#include <kspp/connect/tds/tds_read_cursor.h>
#include <kspp/kspp.h>
#include <chrono>
#include <memory>
#include <glog/logging.h>
#include <boost/bind.hpp>
#include  <kspp/connect/tds/tds_avro_utils.h>

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
      , _id_column(id_column)
      , _ts_column(ts_column)
      , _order_by(__order_by(_ts_column, _id_column))
      , _ts_multiplier(tp.ts_multiplier)
      , _ts_utc_offset(tp.ts_utc_offset){
  }

  void tds_read_cursor::init(DBPROCESS *stream) {
    if (_id_column.size()) {
      std::string simple_name = tds::simple_column_name(_id_column);
      int ix = tds::find_column_by_name(stream, simple_name);
      if (ix >= 0)
        _id_column_index = ix + 1;
      else
        LOG(FATAL) << "could not find id column: " << _id_column;
    }

    if (_ts_column.size()) {
      std::string simple_name = tds::simple_column_name(_ts_column);
      int ix = tds::find_column_by_name(stream, simple_name);
      if (ix >= 0)
        _ts_column_index = ix + 1;
      else
        LOG(FATAL) << "could not find ts column: " << _ts_column;
    }
  }

  void tds_read_cursor::start(int64_t ts) {
    if (!_ts_column.size())
      LOG(FATAL) << "start from ts but no timestamp column";

    _last_ts_ticks = ts;
    _last_ts = std::to_string(_last_ts_ticks);
  }

  void tds_read_cursor::parse(DBPROCESS *stream) {
    if (_ts_column_index > 0) {
      _last_ts_ticks = parse_ts(stream);
      _last_ts = std::to_string(_last_ts_ticks);
    }
    if (_id_column_index > 0)
      _last_id = std::to_string(parse_id(stream));
  }

  int64_t tds_read_cursor::parse_ts(DBPROCESS *stream) {
    assert(_ts_column_index > 0);

    switch (dbcoltype(stream, _ts_column_index)) {
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
        assert(sizeof(v) == dbdatlen(stream, _ts_column_index));
        memcpy(&v, dbdata(stream, _ts_column_index), sizeof(v));
        return v;
      }
        break;

      case tds::SYBINT8: {
        int64_t v;
        assert(sizeof(v) == dbdatlen(stream, _ts_column_index));
        memcpy(&v, dbdata(stream, _ts_column_index), sizeof(v));
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
        LOG(FATAL) << "unexpected / non supported timestamp type e:" << dbcoltype(stream, _ts_column_index);
    }
  }

  int64_t tds_read_cursor::parse_id(DBPROCESS *stream) {
    assert(_id_column_index > 0);

    switch (dbcoltype(stream, _id_column_index)) {
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
        assert(sizeof(v) == dbdatlen(stream, _id_column_index));
        memcpy(&v, dbdata(stream, _id_column_index), sizeof(v));
        return v;
      }
        break;

      case tds::SYBINT8: {
        int64_t v;
        assert(sizeof(v) == dbdatlen(stream, _id_column_index));
        memcpy(&v, dbdata(stream, _id_column_index), sizeof(v));
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
        LOG(FATAL) << "unexpected / non supported id type e:" << dbcoltype(stream, _id_column_index);
    }
  }

  std::string tds_read_cursor::get_where_clause() const {
    // if we are rescraping is active at eof - and then we do not care for id at all.
    // rescrape 0 also handles when we do not have an id field
    if (_eof && _last_ts.size() && tp_.rescrape_policy == kspp::connect::LAST_QUERY_TS)
      return " WHERE " + _ts_column + " >= '" + std::to_string(_last_ts_ticks - tp_.rescrape_ticks) + "' " + _order_by;

    // no data in either id or ts fields?
    if (!_last_id.size() || !_last_ts.size()) {
      if (_last_id.size())
        return " WHERE " + _id_column + " > '" + _last_id + "' " + _order_by;
      else if (_last_ts.size())
        return " WHERE " + _ts_column + " >= '" + _last_ts + "' " + _order_by;
      else
        return "" + _order_by;
    } else {
      return " WHERE (" + _ts_column + " = '" + _last_ts + "' AND " + _id_column + " > '" + _last_id + "') OR (" +
             _ts_column + " > '" + _last_ts + "') " + _order_by;
    }
  }
}