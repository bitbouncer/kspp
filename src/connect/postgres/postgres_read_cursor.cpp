#include <kspp/connect/postgres/postgres_read_cursor.h>
#include <kspp/kspp.h>
#include <chrono>
#include <memory>
#include <glog/logging.h>
#include <boost/bind.hpp>
#include  <kspp/connect/postgres/postgres_avro_utils.h>

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

postgres_read_cursor::postgres_read_cursor(kspp::connect::table_params tp, std::string id_column, std::string ts_column)
      : tp_(tp)
      , id_column_(id_column)
      , ts_column_(ts_column)
      , order_by_(__order_by(ts_column_, id_column_))
      , ts_multiplier_(tp.ts_multiplier)
      , ts_utc_offset_(tp.ts_utc_offset){
    assert(ts_multiplier_>=1);
  }

  void postgres_read_cursor::init(std::shared_ptr<PGresult> result) {
    if (id_column_.size()) {
      std::string simple_name = pq::simple_column_name(id_column_);
      id_column_index_ = PQfnumber(result.get(), simple_name.c_str());
      if (id_column_index_ < 0)
        LOG(FATAL) << "could not find id column: " << id_column_;
    }

    if (ts_column_.size()) {
      std::string simple_name = pq::simple_column_name(ts_column_);
      ts_column_index_ = PQfnumber(result.get(), simple_name.c_str());
      if (ts_column_index_ < 0)
        LOG(FATAL) << "could not find ts column: " << ts_column_;
    }
  }

  void postgres_read_cursor::start(int64_t ts) {
    if (!ts_column_.size())
      LOG(FATAL) << "start from ts but no timestamp column";

    last_ts_ticks_ = ts;
    last_ts_ = std::to_string(last_ts_ticks_);
  }

  void postgres_read_cursor::parse(std::shared_ptr<PGresult> result) {
    if (ts_column_index_ >= 0) {
      last_ts_ = parse_ts(result);
      last_ts_ticks_ = atoll(last_ts_.c_str());
    }
    if (id_column_index_ >= 0)
      last_id_ = parse_id(result);
  }

  std::string postgres_read_cursor::parse_ts(std::shared_ptr<PGresult> result) {
    assert(ts_column_index_ >= 0);
    int nRows = PQntuples(result.get());
    return PQgetvalue(result.get(), nRows - 1, ts_column_index_);
  }

  std::string postgres_read_cursor::parse_id(std::shared_ptr<PGresult> result) {
    assert(id_column_index_ >= 0);
    int nRows = PQntuples(result.get());
    return PQgetvalue(result.get(), nRows - 1, id_column_index_);
  }

  std::string postgres_read_cursor::get_where_clause() const {
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