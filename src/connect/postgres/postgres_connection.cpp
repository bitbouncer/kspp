#include <kspp/connect/postgres/postgres_connection.h>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <glog/logging.h>
#include <kspp/kspp.h>

#define STATEMENT_LOG_BYTES 128

namespace kspp_postgres {

  inline static std::shared_ptr<PGresult> make_shared(PGresult* p) { return std::shared_ptr<PGresult>(p, PQclear); }

  connection::connection(std::string trace_id)
      : _pg_conn(nullptr)
      , _warn_timeout(60000)
      , _trace_id(trace_id) {
    if (!_trace_id.size()) {
      auto uuid = boost::uuids::random_generator();
      _trace_id = to_string(uuid());
    }
    DLOG(INFO) << _trace_id << ", " << BOOST_CURRENT_FUNCTION;
  }

  connection::~connection() {
    disconnect();
    DLOG(INFO) << _trace_id << ", " << BOOST_CURRENT_FUNCTION;
  }

  void connection::close() {
    disconnect();
  }

  int connection::connect(const kspp::connect::connection_params& cp) {
    static_assert(CONNECTION_OK==0, "OK must be 0"); // no surprises

    // make sure we start clean
    disconnect();

    auto t0 = kspp::milliseconds_since_epoch();

    std::string connect_string = "host=" + cp.host +
                                 " port=" + std::to_string(cp.port) +
                                 " user=" + cp.user +
                                 " password=" + cp.password +
                                 " dbname=" + cp.database_name +
                                 " tcp_keepalives_count=5" +      /* test for aws RDS that saeems to have connection issues */
                                 " tcp_keepalives_idle=200" +
                                 " tcp_keepalives_interval=200";

    _pg_conn = PQconnectdb(connect_string.c_str());
    auto status = PQstatus(_pg_conn); //
    int32_t duration = (int32_t) (kspp::milliseconds_since_epoch() - t0);
    if(status == CONNECTION_OK) {
      if(duration > _warn_timeout) {
        LOG(WARNING) << _trace_id << ", postgres::connect - took long time, t=" << duration;
      }

      LOG(INFO) << _trace_id << ", postgres::connect PQconnectdb complete, t=" << duration;
      return 0;
    }
    LOG(ERROR) << _trace_id << ", postgres::connect PQconnectdb failed, status=" << status << ", " << last_error() << ", t=" << duration;
    return status;
  }

  void connection::disconnect() {
    if (_pg_conn)
      PQfinish(_pg_conn);
    _pg_conn = nullptr;
  }

  int connection::set_client_encoding(std::string s) {
    int res = PQsetClientEncoding(_pg_conn, s.c_str());
    if (res==0)
      return 0;
   LOG(ERROR) << _trace_id << " set_client_encoding " << s  << " failed, error: " << last_error();
    return res;
  }

  const std::string& connection::trace_id() const {
    return _trace_id;
  }

  void connection::set_warning_timeout(uint32_t ms) {
    _warn_timeout = ms;
  }

  std::pair<int, std::shared_ptr<PGresult>> connection::exec(std::string statement){
    if (_pg_conn) {
      auto t0 = kspp::milliseconds_since_epoch();
      auto res = make_shared(PQexec(_pg_conn, statement.c_str()));
      auto t1 = kspp::milliseconds_since_epoch();
      auto duration = t1 - t0;
      int status = PQresultStatus(res.get());
      switch(status) {
        case PGRES_EMPTY_QUERY:
        case PGRES_COMMAND_OK:
        case PGRES_TUPLES_OK:
        case PGRES_COPY_OUT:
        case PGRES_COPY_IN:
        case PGRES_COPY_BOTH:
        case PGRES_SINGLE_TUPLE:
          //LOG(INFO) << _trace_id << ", postgres::exec complete, t=" << duration << ", s=" << _current_statement.substr(0, STATEMENT_LOG_BYTES);
          if(duration > _warn_timeout) {
            LOG(WARNING) << _trace_id << ", postgres::exec complete - took long time, t=" << duration << ", s = " << statement.substr(0, STATEMENT_LOG_BYTES);
          }
          return std::make_pair<>(0, std::move(res));
        case PGRES_NONFATAL_ERROR:
        case PGRES_BAD_RESPONSE:
        case PGRES_FATAL_ERROR:
          LOG(ERROR) << _trace_id << ", postgres::exec failed " << last_error() << ", t=" << duration << ", s=" << statement.substr(0, STATEMENT_LOG_BYTES);
          //return std::make_pair<int, std::shared_ptr<PGresult>>(status, std::move(res));
          return std::make_pair<>(status, std::move(res));
        default:
          LOG(WARNING) << _trace_id << ", postgres::exec unknown status code, t=" << duration << ", s=" << statement.substr(0, STATEMENT_LOG_BYTES);
          return std::make_pair<>(status, std::move(res));
          break;
      }
    }
    LOG(FATAL) << "TRYING TO EXEC WITH _pg_conn == nullptr";
    return std::make_pair<>(-1, nullptr);
  }
}
