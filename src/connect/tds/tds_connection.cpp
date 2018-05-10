#include <kspp/connect/tds/tds_connection.h>
#include <future>
#include <boost/bind.hpp>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <glog/logging.h>
#include <utility>


//#include <freetds/tds.h>
//#include <freetds/iconv.h>
//#include <freetds/string.h>
//#include <freetds/convert.h>
//#include <freetds/data.h>


#define STATEMENT_LOG_BYTES 40

static int64_t now() {
  static boost::posix_time::ptime time_t_epoch(boost::gregorian::date(1970, 1, 1));
  boost::posix_time::ptime now = boost::posix_time::microsec_clock::universal_time();
  boost::posix_time::time_duration diff = now - time_t_epoch;
  return diff.total_milliseconds();
}

static int err_handler(DBPROCESS * dbproc, int severity, int dberr, int oserr, char *dberrstr, char *oserrstr)
{
  if (dberr) {
    LOG(ERROR) << "TDS: msg " << dberr << ", Level " << severity << " " << dberrstr;
  } else {
    LOG(ERROR) << "TDS: DB-LIBRARY error: " << dberrstr;
  }

  if (oserr && oserrstr)
    LOG(ERROR) << " (OS error " << oserr << " " << oserrstr;

  return INT_CANCEL;
}

static int msg_handler(DBPROCESS * dbproc, DBINT msgno, int msgstate, int severity, char *msgtext, char *srvname, char *procname, int line)
{
  enum {changed_database = 5701, changed_language = 5703 };

  if (msgno == changed_database || msgno == changed_language)
    return 0;

  if (msgno > 0) {
    LOG(INFO) << "Msg " << (long) msgno << ", Level " << severity << ", State " << msgstate;

    if (strlen(srvname) > 0)
      LOG(INFO) << "Server  " << srvname;
    if (strlen(procname) > 0)
      LOG(INFO) << "Procedure " << procname;
    if (line > 0)
      LOG(INFO) << "Line " << line;
  }
  LOG(INFO) << msgtext;

  if (severity > 10) {
    LOG(FATAL) << "severity " << severity << " > 10, exiting";
  }

  return 0;
}


void tds_global_init() {
  static bool is_init=false;

  if (!is_init) {
    is_init=true;
    /* Initialize db-lib */
    auto erc = dbinit();
    if (erc == FAIL) {
      LOG(FATAL) << "dbinit() failed";
    }

/* Install our error and message handlers */
    dberrhandle(err_handler);
    dbmsghandle(msg_handler);
  }
}

namespace kspp_tds {
  //inline static std::shared_ptr<PGresult> make_shared(PGresult* p) { return std::shared_ptr<PGresult>(p, PQclear); }

  connection::connection(std::string trace_id) :
      //_fg_ios(fg),
      //_bg_ios(bg),
      //_socket(fg),
      dbproc_(nullptr),
      login_(nullptr),
      _warn_timeout(60000),
      _trace_id(trace_id) {
    tds_global_init();

    login_ = dblogin();
    if (!login_)
      LOG(FATAL) << "unable to allocate login structure";


    if (!_trace_id.size()) {
      auto uuid = boost::uuids::random_generator();
      _trace_id = to_string(uuid());
    }
    LOG(INFO) << _trace_id << ", " << BOOST_CURRENT_FUNCTION;
  }

  connection::~connection() {
    LOG(INFO) << _trace_id << ", " << BOOST_CURRENT_FUNCTION;
    //PQfinish(_pg_conn);
    dbproc_ = nullptr;
  }

  /*
   * std::string connection::user_name() const { return PQuser(_pg_conn); }
  std::string connection::password() const { return PQpass(_pg_conn); }
  std::string connection::host_name() const { return PQhost(_pg_conn); }
  std::string connection::port() const { return PQport(_pg_conn); }
  std::string connection::options() const { return PQoptions(_pg_conn); }
  bool        connection::good() const { return (PQstatus(_pg_conn) == CONNECTION_OK); }
  std::string connection::last_error() const { return PQerrorMessage(_pg_conn); }
  uint32_t    connection::backend_pid() const { return (uint32_t) PQbackendPID(_pg_conn); }
  int         connection::socket() const { return PQsocket(_pg_conn); }
  bool        connection::set_client_encoding(std::string s) { return (PQsetClientEncoding(_pg_conn, s.c_str()) == 0); }
   */
  std::string connection::trace_id() const { return _trace_id; }
  void        connection::set_warning_timout(uint32_t ms) { _warn_timeout = ms; }

  // connect is a blocking thing - pass this to bg thread pool
  //void connection::connect(std::string connect_string, on_connect_callback cb) {
  //  auto self(shared_from_this()); // keeps connection alive until cb is done
  //  _bg_ios.post(boost::bind(&connection::_bg_connect, this, self, connect_string, cb));
  //}

  //void connection::connect(on_connect_callback cb) {
  //  auto self(shared_from_this()); // keeps connection alive until cb is done
  //  _bg_ios.post(boost::bind(&connection::_bg_connect, this, self, "", cb));
  //}

  int connection::connect(std::string host, std::string username, std::string password, std::string database) {
    DBSETLAPP(login_, "kspp-tds-connection");
    DBSETLUSER(login_, username.c_str());
    DBSETLHOST(login_, host.c_str());
    DBSETLPWD(login_, password.c_str());
    DBSETLDBNAME(login_, database.c_str()); // maybe optional


    if ((dbproc_ = dbopen(login_,host.c_str())) == NULL)
      LOG(FATAL) << " cannot connect to " << host;

    LOG(INFO) << "connected to " << host <<  " user: " << username << ", database: " << database;
  }

  void connection::close()
  {
    LOG(WARNING) << _trace_id << " tbs::close - NOT IMPLEMENTED";
  }


  std::pair<int, DBPROCESS*> connection::exec(std::string statement){
    DLOG(INFO) << statement;
    dbfreebuf(dbproc_);

    auto erc = dbcmd(dbproc_, statement.c_str());
    if (erc == FAIL) {
     LOG(FATAL) << "dbcmd() failed - exiting";
    }

    if (dbsqlexec(dbproc_) == FAIL) {
      LOG(ERROR) << "dbsqlexec failed ,statement: " << statement;
      return std::make_pair<int, DBPROCESS*>(-1, nullptr);
    }
    return std::make_pair(0, this->dbproc_);
  }


  // connect syncrounous and run callcack from fg thread event loop
//  void connection::_bg_connect(std::shared_ptr<connection> self, std::string connect_string, on_connect_callback cb) {
//    _start_ts = now();
//    _pg_conn = PQconnectdb(connect_string.c_str());
//    auto status = PQstatus(_pg_conn); //
//    int32_t duration = (int32_t) (now() - _start_ts);
//    if(status == CONNECTION_OK) {
//      if(duration > _warn_timeout) {
//        LOG(WARNING) << _trace_id << ", postgres::connect - took long time, t=" << duration;
//      }
//
//      LOG(INFO) << _trace_id << ", postgres::connect PQconnectdb complete, t=" << duration;
//      _socket.assign(boost::asio::ip::tcp::v4(), socket());
//      _fg_ios.post([this, self, cb]() { cb(0); });
//      return;
//    }
//    LOG(ERROR) << _trace_id << ", postgres::connect PQconnectdb failed, status=" << status << ", " << last_error() << ", t=" << duration;
//    _fg_ios.post([this, self, status, cb]() { cb(status); });
//  }

//  void connection::exec(std::string statement, on_query_callback cb) {
//    //LOG(INFO) << _trace_id << ", " << BOOST_CURRENT_FUNCTION << ", s=" << statement.substr(0, STATEMENT_LOG_BYTES);
//    auto self(shared_from_this()); // keeps connection alive until cb is done
//    _start_ts = now();
//    _current_statement = statement;
//    if(PQsendQuery(_pg_conn, statement.c_str()) == 0) // 1 os good, 0 is bad...
//    {
//      LOG(ERROR) << _trace_id << ", postgres::exec PQsendQuery failed fast: s=" << statement.substr(0, STATEMENT_LOG_BYTES);
//      _fg_ios.post([this, self, cb]() { cb(PGRES_FATAL_ERROR, NULL); });
//      return;
//    }
//    _socket.async_read_some(boost::asio::null_buffers(), boost::bind(&connection::_fg_socket_rx_cb, this, boost::asio::placeholders::error, self, cb));
//  }
//
//
//  std::pair<int, std::shared_ptr<PGresult>> connection::exec(std::string statement) {
//    std::promise<std::pair<int, std::shared_ptr<PGresult>>> p;
//    std::future<std::pair<int, std::shared_ptr<PGresult>>>  f = p.get_future();
//    exec(statement, [&p](int ec, std::shared_ptr<PGresult> res) {
//      std::pair<int, std::shared_ptr<PGresult>> val(ec, res);
//      p.set_value(val);
//    });
//    f.wait();
//    return f.get();
//  }

//  void connection::_fg_socket_rx_cb(const boost::system::error_code& ec, std::shared_ptr<connection> self, on_query_callback cb) {
//    //LOG(INFO) << _trace_id << ", " << BOOST_CURRENT_FUNCTION;
//    if(ec) {
//      LOG(WARNING) << _trace_id << ", postgres::exec asio ec:" << ec.message();
//      cb(ec.value(), NULL);
//      return;
//    }
//
//    int res = PQconsumeInput(_pg_conn);
//    if(!res) {
//      LOG(WARNING) << _trace_id << ", postgres::exec PQconsumeInput read error";
//      cb(PGRES_FATAL_ERROR, NULL); // we reuse a error code here...
//      return;
//    }
//
//    //done yet?
//    //we are looking for the second last result...
//    //the last one is a null result.
//
//    while(true) {
//      if(PQisBusy(_pg_conn)) {
//        //LOG(INFO) << _trace_id << ", " << BOOST_CURRENT_FUNCTION << ", PQisBusy() - reading more";
//        _socket.async_read_some(boost::asio::null_buffers(), boost::bind(&connection::_fg_socket_rx_cb, this, boost::asio::placeholders::error, self, cb));
//        return;
//      }
//      //LOG(INFO) << _trace_id << ", " << BOOST_CURRENT_FUNCTION << ", parsing result";
//      auto r = make_shared(PQgetResult(_pg_conn));
//      if(r.get() == NULL)
//        break;
//      _results.push_back(r);
//    }
//
//    int32_t duration = (int32_t) (now() - _start_ts);
//
//    // if we got a NULL here then we are ready to issue another async exec....
//
//    if(_results.size() == 0) {
//      LOG(ERROR) << _trace_id << ", postgres::exec returned no result, t=" << duration << ", s=" << _current_statement.substr(0, STATEMENT_LOG_BYTES);
//      cb(PGRES_FATAL_ERROR, NULL); // we reuse a error code here...
//      return;
//    }
//
//    std::shared_ptr<PGresult> last_result = *_results.rbegin();
//    _results.clear();
//
//    int status = PQresultStatus(last_result.get());
//    switch(status) {
//      case PGRES_EMPTY_QUERY:
//      case PGRES_COMMAND_OK:
//      case PGRES_TUPLES_OK:
//      case PGRES_COPY_OUT:
//      case PGRES_COPY_IN:
//      case PGRES_NONFATAL_ERROR:
//      case PGRES_COPY_BOTH:
//      case PGRES_SINGLE_TUPLE:
//        //LOG(INFO) << _trace_id << ", postgres::exec complete, t=" << duration << ", s=" << _current_statement.substr(0, STATEMENT_LOG_BYTES);
//        if(duration > _warn_timeout) {
//          LOG(WARNING) << _trace_id << ", postgres::exec complete - took long time, t=" << duration << ", s = " << _current_statement.substr(0, STATEMENT_LOG_BYTES);
//        }
//        cb(0, std::move(last_result));
//        break;
//      case PGRES_BAD_RESPONSE:
//      case PGRES_FATAL_ERROR:
//        LOG(ERROR) << _trace_id << ", postgres::exec failed " << last_error() << ", t=" << duration << ", s=" << _current_statement.substr(0, STATEMENT_LOG_BYTES);
//        cb(status, std::move(last_result));
//        break;
//      default:
//        LOG(WARNING) << _trace_id << ", postgres::exec unknown status code, t=" << duration << ", s=" << _current_statement.substr(0, STATEMENT_LOG_BYTES);
//        cb(status, std::move(last_result));
//        break;
//    }
  }