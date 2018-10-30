#include <chrono>
#include <memory>
#include <kspp/impl/queue.h>
#include <kspp/connect/tds/tds_connection.h>
#include <kspp/topology.h>
#include <kspp/avro/generic_avro.h>

#pragma once

namespace kspp {
  class tds_read_cursor {
  public:
    tds_read_cursor(kspp::connect::table_params tp, std::string id_column, std::string ts_column);

    void init(DBPROCESS *stream);

    void start(int64_t ts);

    void parse(DBPROCESS *stream);

    std::string get_where_clause() const;

    std::string last_ts() const { return last_ts_; }

    inline int64_t last_tick() const { return last_ts_ticks_; }

    void set_eof(bool state) { _eof = state; }

  private:

    int64_t parse_id(DBPROCESS *stream);

    int64_t parse_ts(DBPROCESS *stream);

    const kspp::connect::table_params tp_;
    bool _eof = false;
    const std::string _id_column;
    const std::string _ts_column;
    const std::string _order_by;
    int ts_column_index_ = -1;
    int id_column_index_ = -1;
    int64_t last_ts_ticks_ = INT64_MIN;
    std::string last_ts_;
    std::string last_id_;
  };
}