#include <chrono>
#include <memory>
#include <kspp/topology.h>
#include <kspp/avro/generic_avro.h>
#include <kspp/connect/connection_params.h>
#include <kspp-pg/postgres_connection.h>

#pragma once

namespace kspp {
  class postgres_read_cursor {
  public:
    postgres_read_cursor(kspp::connect::table_params tp, std::string id_column, std::string ts_column);

    void init(std::shared_ptr<PGresult> result);

    void start(int64_t ts);

    void parse(std::shared_ptr<PGresult> result);

    std::string get_where_clause() const;

    std::string last_ts() const { return last_ts_; }

    inline int64_t last_tick() const { return last_ts_ticks_; }

    inline int64_t last_ts_ms() const {
      return (last_ts_ticks_ >= 0) ? ts_utc_offset_ + (last_ts_ticks_ * ts_multiplier_) : 0;
    }

    void set_eof(bool state) { eof_ = state; }

  private:

    std::string parse_id(std::shared_ptr<PGresult> result);

    std::string parse_ts(std::shared_ptr<PGresult> result);

    const kspp::connect::table_params tp_;
    bool eof_ = false;
    const std::string id_column_;
    const std::string ts_column_;
    const std::string order_by_;
    int ts_column_index_ = -1;
    int id_column_index_ = -1;
    int64_t last_ts_ticks_ = INT64_MIN;
    std::string last_ts_;
    std::string last_id_;

    int ts_multiplier_ = 0;
    int ts_utc_offset_ = 0;
  };
}