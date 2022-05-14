#include <kspp-pg/postgres_connection.h>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <glog/logging.h>
#include <kspp/kspp.h>

#define STATEMENT_LOG_BYTES 128

namespace kspp_postgres {

  inline static std::shared_ptr<PGresult> make_shared(PGresult *p) { return std::shared_ptr<PGresult>(p, PQclear); }

  connection::connection(std::string trace_id)
      : trace_id_(trace_id) {
    if (!trace_id_.size()) {
      auto uuid = boost::uuids::random_generator();
      trace_id_ = to_string(uuid());
    }
    DLOG(INFO) << trace_id_ << ", " << BOOST_CURRENT_FUNCTION;
  }

  connection::~connection() {
    disconnect();
    DLOG(INFO) << trace_id_ << ", " << BOOST_CURRENT_FUNCTION;
  }

  void connection::close() {
    disconnect();
  }

  int connection::connect(const kspp::connect::connection_params &cp) {
    static_assert(CONNECTION_OK == 0, "OK must be 0"); // no surprises

    // make sure we start clean
    disconnect();

    auto t0 = kspp::milliseconds_since_epoch();

    std::string connect_string = "host=" + cp.host +
                                 " port=" + std::to_string(cp.port) +
                                 " user=" + cp.user +
                                 " password=" + cp.password +
                                 " dbname=" + cp.database_name +
                                 " keepalives_count=5" +
                                 /* test for aws RDS that saeems to have connection issues */
                                 " keepalives_idle=200" +
                                 " keepalives_interval=200";

    pg_conn_ = PQconnectdb(connect_string.c_str());
    auto status = PQstatus(pg_conn_); //
    int32_t duration = (int32_t) (kspp::milliseconds_since_epoch() - t0);
    if (status == CONNECTION_OK) {
      if (duration > warn_timeout_) {
        LOG(WARNING) << trace_id_ << ", postgres::connect - took long time, t=" << duration;
      }

      LOG(INFO) << trace_id_ << ", postgres::connect PQconnectdb complete, t=" << duration;
      return 0;
    }
    LOG(ERROR) << trace_id_ << ", postgres::connect PQconnectdb failed, status=" << status << ", " << last_error()
               << ", t=" << duration;
    return status;
  }

  void connection::disconnect() {
    if (pg_conn_)
      PQfinish(pg_conn_);
    pg_conn_ = nullptr;
  }

  int connection::set_client_encoding(std::string s) {
    int res = PQsetClientEncoding(pg_conn_, s.c_str());
    if (res == 0)
      return 0;
    LOG(ERROR) << trace_id_ << " set_client_encoding " << s << " failed, error: " << last_error();
    return res;
  }

  const std::string &connection::trace_id() const {
    return trace_id_;
  }

  void connection::set_warning_timeout(uint32_t ms) {
    warn_timeout_ = ms;
  }

  std::pair<int, std::shared_ptr<PGresult>> connection::exec(std::string statement) {
    if (pg_conn_) {
      auto t0 = kspp::milliseconds_since_epoch();
      auto res = make_shared(PQexec(pg_conn_, statement.c_str()));
      auto t1 = kspp::milliseconds_since_epoch();
      auto duration = t1 - t0;
      int status = PQresultStatus(res.get());
      switch (status) {
        case PGRES_EMPTY_QUERY:
        case PGRES_COMMAND_OK:
        case PGRES_TUPLES_OK:
        case PGRES_COPY_OUT:
        case PGRES_COPY_IN:
        case PGRES_COPY_BOTH:
        case PGRES_SINGLE_TUPLE:
          //LOG(INFO) << _trace_id << ", postgres::exec complete, t=" << duration << ", s=" << _current_statement.substr(0, STATEMENT_LOG_BYTES);
          if (duration > warn_timeout_) {
            LOG(WARNING) << trace_id_ << ", postgres::exec complete - took long time, t=" << duration << ", s = "
                         << statement.substr(0, STATEMENT_LOG_BYTES);
          }
          return std::make_pair<>(0, std::move(res));
        case PGRES_NONFATAL_ERROR:
        case PGRES_BAD_RESPONSE:
        case PGRES_FATAL_ERROR:
          LOG(ERROR) << trace_id_ << ", postgres::exec failed " << last_error() << ", t=" << duration << ", s="
                     << statement.substr(0, STATEMENT_LOG_BYTES);
          return std::make_pair<>(status, std::move(res));
        default:
          LOG(WARNING) << trace_id_ << ", postgres::exec unknown status code, t=" << duration << ", s="
                       << statement.substr(0, STATEMENT_LOG_BYTES);
          return std::make_pair<>(status, std::move(res));
          break;
      }
    }
    LOG(FATAL) << "TRYING TO EXEC WITH _pg_conn == nullptr";
    return std::make_pair<>(-1, nullptr);
  }
}
