#include <memory>
#include <utility>
#include <functional>
#include <deque>
#include <boost/asio.hpp>
#include <postgresql/libpq-fe.h>
#pragma once

//inspiration
//https://github.com/brianc/node-libpq

namespace postgres_asio {
  class connection : public std::enable_shared_from_this<connection> {
  public:
    typedef std::function<void(int ec)> on_connect_callback;
    typedef std::function<void(int ec, std::shared_ptr<PGresult>)> on_query_callback;

    connection(boost::asio::io_service& fg, boost::asio::io_service& bg, std::string trace_id = "");
    ~connection();

    void close();

    /*
        host     -- host to connect to.If a non-zero-length string is specified, TCP/IP communication is used.Without a host name, libpq will connect using a local Unix domain socket.
        port     -- port number to connect to at the server host, or socket filename extension for Unix-domain connections.
        dbname   -- database name.
        user     -- user name for authentication.
        password -- password used if the backend demands password authentication.
        options  -- trace/debug options to send to backend.
        tty      -- file or tty for optional debug output from backend.

        async connect
        */
    void connect(std::string connect_string, on_connect_callback cb);
    //async connect
    void connect(on_connect_callback cb);

    //sync connect
    int connect(std::string connect_string);
    //sync connect
    int connect();

    //status (non blocking)
    std::string user_name() const;
    std::string password() const;
    std::string host_name() const;
    std::string port() const;
    std::string options() const;
    bool        good() const;
    std::string last_error() const;
    uint32_t    backend_pid() const;

    bool        set_client_encoding(std::string s);
    std::string trace_id() const;
    void        set_warning_timout(uint32_t ms);

    //async exec
    void exec(std::string statement, on_query_callback cb);
    //sync exec
    std::pair<int, std::shared_ptr<PGresult>> exec(std::string statement);
  private:
    int socket() const;

    void _bg_connect(std::shared_ptr<connection> self, std::string connect_string, on_connect_callback cb);
    void _fg_socket_rx_cb(const boost::system::error_code& e, std::shared_ptr<connection>, on_query_callback cb);

    boost::asio::io_service&              _fg_ios;
    boost::asio::io_service&              _bg_ios;
    boost::asio::ip::tcp::socket          _socket;
    PGconn*                               _pg_conn;
    std::string                           _trace_id;
    int64_t                               _start_ts;
    int32_t                               _warn_timeout;
    std::string                           _current_statement;
    std::deque<std::shared_ptr<PGresult>> _results;
  };

  /*  class connection_pool
    {
    public:
    connection_pool(boost::asio::io_service& fg, boost::asio::io_service& bg);
    boost::shared_ptr<postgres_asio::connection> create();
    void release(boost::shared_ptr<postgres_asio::connection>);
    protected:
    boost::asio::io_service& _fg_ios;
    boost::asio::io_service& _bg_ios;
    };*/
}
