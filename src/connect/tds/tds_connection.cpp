#include <kspp/connect/tds/tds_connection.h>
#include <future>
#include <boost/bind.hpp>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <glog/logging.h>
#include <utility>

#define STATEMENT_LOG_BYTES 40

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

  connection::connection(std::string trace_id) :
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
  }