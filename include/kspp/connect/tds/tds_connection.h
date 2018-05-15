#include <memory>
#include <utility>
#include <functional>
#include <deque>
#include <boost/asio.hpp>
#include <sybfront.h>
#include <sybdb.h>
#pragma once

//inspiration
//http://www.freetds.org/software.html

namespace kspp_tds {
  class connection : public std::enable_shared_from_this<connection> {
  public:
    typedef std::function<void(int ec, DBPROCESS*)> on_query_callback;

    connection(std::string trace_id = "");
    ~connection();

    void close();

    int connect(std::string host, std::string username, std::string password, std::string database);

    void disconnect();

    inline bool connected() const {
      return (dbproc_!=nullptr);
    }

    bool set_client_encoding(std::string s);

    std::string trace_id() const;
    void set_warning_timeout(uint32_t ms);

    std::pair<int, DBPROCESS*> exec(std::string statement);
  private:
    LOGINREC* login_;
    DBPROCESS* dbproc_;
    std::string _trace_id;
    int64_t _start_ts;
    int32_t _warn_timeout;
    std::string _current_statement;
  };
}
