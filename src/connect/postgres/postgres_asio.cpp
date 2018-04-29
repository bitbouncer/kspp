#include <kspp/connect/postgres/postgres_asio.h>
#include <future>
#include <boost/bind.hpp>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <glog/logging.h>

//inspiration...
//https://github.com/brianc/node-libpq

#define STATEMENT_LOG_BYTES 40

static int64_t now() {
  static boost::posix_time::ptime time_t_epoch(boost::gregorian::date(1970, 1, 1));
  boost::posix_time::ptime now = boost::posix_time::microsec_clock::universal_time();
  boost::posix_time::time_duration diff = now - time_t_epoch;
  return diff.total_milliseconds();
}

namespace postgres_asio {
  inline static std::shared_ptr<PGresult> make_shared(PGresult* p) { return std::shared_ptr<PGresult>(p, PQclear); }

  connection::connection(boost::asio::io_service& fg, boost::asio::io_service& bg, std::string trace_id) :
      _fg_ios(fg),
      _bg_ios(bg),
      _socket(fg),
      _pg_conn(NULL),
      _warn_timeout(60000),
      _trace_id(trace_id) {
    if (!_trace_id.size()) {
      auto uuid = boost::uuids::random_generator();
      _trace_id = to_string(uuid());
    }
    LOG(INFO) << _trace_id << ", " << BOOST_CURRENT_FUNCTION;
  }

  connection::~connection() {
    LOG(INFO) << _trace_id << ", " << BOOST_CURRENT_FUNCTION;
    PQfinish(_pg_conn);
  }

  std::string connection::user_name() const { return PQuser(_pg_conn); }
  std::string connection::password() const { return PQpass(_pg_conn); }
  std::string connection::host_name() const { return PQhost(_pg_conn); }
  std::string connection::port() const { return PQport(_pg_conn); }
  std::string connection::options() const { return PQoptions(_pg_conn); }
  bool        connection::good() const { return (PQstatus(_pg_conn) == CONNECTION_OK); }
  std::string connection::last_error() const { return PQerrorMessage(_pg_conn); }
  uint32_t    connection::backend_pid() const { return (uint32_t) PQbackendPID(_pg_conn); }
  int         connection::socket() const { return PQsocket(_pg_conn); }
  bool        connection::set_client_encoding(std::string s) { return (PQsetClientEncoding(_pg_conn, s.c_str()) == 0); }
  std::string connection::trace_id() const { return _trace_id; }
  void        connection::set_warning_timout(uint32_t ms) { _warn_timeout = ms; }

  // connect is a blocking thing - pass this to bg thread pool
  void connection::connect(std::string connect_string, on_connect_callback cb) {
    auto self(shared_from_this()); // keeps connection alive until cb is done
    _bg_ios.post(boost::bind(&connection::_bg_connect, this, self, connect_string, cb));
  }

  void connection::connect(on_connect_callback cb) {
    auto self(shared_from_this()); // keeps connection alive until cb is done
    _bg_ios.post(boost::bind(&connection::_bg_connect, this, self, "", cb));
  }

  int connection::connect(std::string connect_string) {
    std::promise<int> p;
    std::future<int>  f = p.get_future();
    connect(connect_string, [&p](int ec) {
      p.set_value(ec);
    });
    f.wait();
    return f.get();
  }

  int connection::connect() {
    std::promise<int> p;
    std::future<int>  f = p.get_future();
    connect([&p](int ec) {
      p.set_value(ec);
    });
    f.wait();
    return f.get();
  }

  void connection::close()
  {
    LOG(WARNING) << _trace_id << " postgres::close - NOT IMPLEMENTED";
  }

  // connect syncrounous and run callcack from fg thread event loop
  void connection::_bg_connect(std::shared_ptr<connection> self, std::string connect_string, on_connect_callback cb) {
    _start_ts = now();
    _pg_conn = PQconnectdb(connect_string.c_str());
    auto status = PQstatus(_pg_conn); //
    int32_t duration = (int32_t) (now() - _start_ts);
    if(status == CONNECTION_OK) {
      if(duration > _warn_timeout) {
        LOG(WARNING) << _trace_id << ", postgres::connect - took long time, t=" << duration;
      }

      LOG(INFO) << _trace_id << ", postgres::connect PQconnectdb complete, t=" << duration;
      _socket.assign(boost::asio::ip::tcp::v4(), socket());
      _fg_ios.post([this, self, cb]() { cb(0); });
      return;
    }
    LOG(ERROR) << _trace_id << ", postgres::connect PQconnectdb failed, status=" << status << ", " << last_error() << ", t=" << duration;
    _fg_ios.post([this, self, status, cb]() { cb(status); });
  }

  void connection::exec(std::string statement, on_query_callback cb) {
    //LOG(INFO) << _trace_id << ", " << BOOST_CURRENT_FUNCTION << ", s=" << statement.substr(0, STATEMENT_LOG_BYTES);
    auto self(shared_from_this()); // keeps connection alive until cb is done
    _start_ts = now();
    _current_statement = statement;
    if(PQsendQuery(_pg_conn, statement.c_str()) == 0) // 1 os good, 0 is bad...
    {
      LOG(ERROR) << _trace_id << ", postgres::exec PQsendQuery failed fast: s=" << statement.substr(0, STATEMENT_LOG_BYTES);
      _fg_ios.post([this, self, cb]() { cb(PGRES_FATAL_ERROR, NULL); });
      return;
    }
    _socket.async_read_some(boost::asio::null_buffers(), boost::bind(&connection::_fg_socket_rx_cb, this, boost::asio::placeholders::error, self, cb));
  }


  std::pair<int, std::shared_ptr<PGresult>> connection::exec(std::string statement) {
    std::promise<std::pair<int, std::shared_ptr<PGresult>>> p;
    std::future<std::pair<int, std::shared_ptr<PGresult>>>  f = p.get_future();
    exec(statement, [&p](int ec, std::shared_ptr<PGresult> res) {
      std::pair<int, std::shared_ptr<PGresult>> val(ec, res);
      p.set_value(val);
    });
    f.wait();
    return f.get();
  }

  void connection::_fg_socket_rx_cb(const boost::system::error_code& ec, std::shared_ptr<connection> self, on_query_callback cb) {
    //LOG(INFO) << _trace_id << ", " << BOOST_CURRENT_FUNCTION;
    if(ec) {
      LOG(WARNING) << _trace_id << ", postgres::exec asio ec:" << ec.message();
      cb(ec.value(), NULL);
      return;
    }

    int res = PQconsumeInput(_pg_conn);
    if(!res) {
      LOG(WARNING) << _trace_id << ", postgres::exec PQconsumeInput read error";
      cb(PGRES_FATAL_ERROR, NULL); // we reuse a error code here...
      return;
    }

    //done yet?
    //we are looking for the second last result...
    //the last one is a null result.

    while(true) {
      if(PQisBusy(_pg_conn)) {
        //LOG(INFO) << _trace_id << ", " << BOOST_CURRENT_FUNCTION << ", PQisBusy() - reading more";
        _socket.async_read_some(boost::asio::null_buffers(), boost::bind(&connection::_fg_socket_rx_cb, this, boost::asio::placeholders::error, self, cb));
        return;
      }
      //LOG(INFO) << _trace_id << ", " << BOOST_CURRENT_FUNCTION << ", parsing result";
      auto r = make_shared(PQgetResult(_pg_conn));
      if(r.get() == NULL)
        break;
      _results.push_back(r);
    }

    int32_t duration = (int32_t) (now() - _start_ts);

    // if we got a NULL here then we are ready to issue another async exec....

    if(_results.size() == 0) {
      LOG(ERROR) << _trace_id << ", postgres::exec returned no result, t=" << duration << ", s=" << _current_statement.substr(0, STATEMENT_LOG_BYTES);
      cb(PGRES_FATAL_ERROR, NULL); // we reuse a error code here...
      return;
    }

    std::shared_ptr<PGresult> last_result = *_results.rbegin();
    _results.clear();

    int status = PQresultStatus(last_result.get());
    switch(status) {
      case PGRES_EMPTY_QUERY:
      case PGRES_COMMAND_OK:
      case PGRES_TUPLES_OK:
      case PGRES_COPY_OUT:
      case PGRES_COPY_IN:
      case PGRES_NONFATAL_ERROR:
      case PGRES_COPY_BOTH:
      case PGRES_SINGLE_TUPLE:
        //LOG(INFO) << _trace_id << ", postgres::exec complete, t=" << duration << ", s=" << _current_statement.substr(0, STATEMENT_LOG_BYTES);
        if(duration > _warn_timeout) {
          LOG(WARNING) << _trace_id << ", postgres::exec complete - took long time, t=" << duration << ", s = " << _current_statement.substr(0, STATEMENT_LOG_BYTES);
        }
        cb(0, std::move(last_result));
        break;
      case PGRES_BAD_RESPONSE:
      case PGRES_FATAL_ERROR:
        LOG(ERROR) << _trace_id << ", postgres::exec failed " << last_error() << ", t=" << duration << ", s=" << _current_statement.substr(0, STATEMENT_LOG_BYTES);
        cb(status, std::move(last_result));
        break;
      default:
        LOG(WARNING) << _trace_id << ", postgres::exec unknown status code, t=" << duration << ", s=" << _current_statement.substr(0, STATEMENT_LOG_BYTES);
        cb(status, std::move(last_result));
        break;
    }
  }


  //connection_pool::connection_pool(boost::asio::io_service& fg, boost::asio::io_service& bg) : _fg_ios(fg), _bg_ios(bg)
  //{
  //}

  ////TBD reuse connections.
  //std::shared_ptr<postgres_asio::connection> connection_pool::create()
  //{
  //    return std::make_shared<postgres_asio::connection>(_fg_ios, _bg_ios);
  //}
  //
  //// TBD 
  //void connection_pool::release(boost::shared_ptr<postgres_asio::connection>)
  //{
  //}
}

