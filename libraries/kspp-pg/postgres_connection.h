#include <memory>
#include <utility>
#include <functional>
#include <libpq-fe.h>
#include <kspp/connect/connection_params.h>

#pragma once

//inspiration
//http://www.freetds.org/software.html

namespace kspp_postgres {
  class connection : public std::enable_shared_from_this<connection> {
  public:
    connection(std::string trace_id = "");

    ~connection();

    void close();

    int connect(const kspp::connect::connection_params &cp);

    void disconnect();

    inline int connected() const {
      return (pg_conn_ && (PQstatus(pg_conn_) == CONNECTION_OK));
    }

    int set_client_encoding(std::string s);

    const std::string &trace_id() const;

    void set_warning_timeout(uint32_t ms);

    std::pair<int, std::shared_ptr<PGresult>> exec(std::string statement);

    std::string last_error() const {
      return PQerrorMessage(pg_conn_);
    }

  private:
    PGconn *pg_conn_ = nullptr;
    std::string trace_id_;
    int32_t warn_timeout_ = 60000;
  };
}
