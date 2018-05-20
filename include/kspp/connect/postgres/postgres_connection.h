#include <memory>
#include <utility>
#include <functional>
#include <postgresql/libpq-fe.h>
#pragma once

//inspiration
//http://www.freetds.org/software.html

namespace kspp_postgres {
  class connection : public std::enable_shared_from_this<connection> {
  public:
    connection(std::string trace_id = "");
    ~connection();

    void close();

    int connect(std::string host, int port, std::string username, std::string password, std::string database);

    void disconnect();

    inline int connected() const {
      return (_pg_conn && (PQstatus(_pg_conn) == CONNECTION_OK));
    }

    int set_client_encoding(std::string s);

    const std::string& trace_id() const;

    void set_warning_timeout(uint32_t ms);

    std::pair<int, std::shared_ptr<PGresult>> exec(std::string statement);

    std::string last_error() const {
      return PQerrorMessage(_pg_conn);
    }

  private:
    PGconn* _pg_conn;
    std::string _trace_id;
    int32_t _warn_timeout;
  };
}
