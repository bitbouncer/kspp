#include <kspp/connect/tds/tds_consumer.h>
#include <kspp/kspp.h>
#include <chrono>
#include <memory>
#include <glog/logging.h>
#include <boost/bind.hpp>

namespace kspp {
  tds_consumer::tds_consumer(int32_t partition,
                             std::string table,
                             std::string consumer_group,
                             std::string host,
                             std::string user,
                             std::string password,
                             std::string database,
                             std::string id_column,
                             std::string ts_column,
                             size_t max_items_in_fetch)
      : _bg([this] { _thread(); } )
      , _connection(std::make_shared<kspp_tds::connection>())
      , _table(table)
      , _partition(partition)
      , _consumer_group(consumer_group)
      , host_(host)
      , user_(user)
      , password_(password)
      , database_(database)
      , _id_column(id_column)
      , _ts_column(ts_column)
      , _max_items_in_fetch(max_items_in_fetch)
      , _can_be_committed(0)
      , _last_committed(-1)
      , _max_pending_commits(0)
      , _msg_cnt(0)
      , _msg_bytes(0)
      , _good(true)
      , _closed(false)
      , _eof(false)
      , _connected(false) {
  }

  tds_consumer::~tds_consumer(){
    if (!_closed)
      close();

    //_fg_work.reset();
    //_bg_work.reset();
    //bg_ios.stop();
    //fg_ios.stop();
    _bg.join();
    //_fg.join();

    _connection->close();
    _connection = nullptr;
  }

  void tds_consumer::close(){
    if (_closed)
      return;
    _closed = true;

    if (_connection) {
      _connection->close();
      LOG(INFO) << "postgres_consumer table:" << _table << ":" << _partition << ", closed - consumed " << _msg_cnt << " messages (" << _msg_bytes << " bytes)";
    }

    _connected = false;
  }

  bool tds_consumer::initialize() {
    _connection->connect(host_, user_, password_, database_);
    _connected = true;
  }

  /*
   * std::shared_ptr<PGresult> tds_consumer::consume(){
    if (_queue.empty())
      return nullptr;
    auto p = _queue.pop_and_get();
    return p;
  }
   */

  void tds_consumer::start(int64_t offset){
    if (offset == kspp::OFFSET_STORED) {
      //TODO not implemented yet
      /*
       * if (_config->get_cluster_metadata()->consumer_group_exists(_consumer_group, 5s)) {
        DLOG(INFO) << "kafka_consumer::start topic:" << _topic << ":" << _partition  << " consumer group: " << _consumer_group << " starting from OFFSET_STORED";
      } else {
        //non existing consumer group means start from beginning
        LOG(INFO) << "kafka_consumer::start topic:" << _topic << ":" << _partition  << " consumer group: " << _consumer_group << " missing, OFFSET_STORED failed -> starting from OFFSET_BEGINNING";
        offset = kspp::OFFSET_BEGINNING;
      }
       */
    } else if (offset == kspp::OFFSET_BEGINNING) {
      DLOG(INFO) << "postgres_consumer::start table:" << _table << ":" << _partition  << " consumer group: " << _consumer_group << " starting from OFFSET_BEGINNING";
    } else if (offset == kspp::OFFSET_END) {
      DLOG(INFO) << "postgres_consumer::start table:" << _table << ":" << _partition  << " consumer group: " << _consumer_group << " starting from OFFSET_END";
    } else {
      DLOG(INFO) << "postgres_consumer::start table:" << _table << ":" << _partition  << " consumer group: " << _consumer_group << " starting from fixed offset: " << offset;
    }

    initialize();
    //update_eof();
  }

  void tds_consumer::stop(){

  }

  int32_t tds_consumer::commit(int64_t offset, bool flush){

  }

  int tds_consumer::update_eof() {

  }

//  void tds_consumer::connect_async() {
//    DLOG(INFO) << "connecting : connect_string: " <<  _connect_string;
//    _connection->connect(_connect_string, [this](int ec) {
//      if (!ec) {
//        DLOG(INFO) << "connected";
//        bool r1 = _connection->set_client_encoding("UTF8");
//        _good = true;
//        _connected = true;
//        _eof = true;
//        select_async();
//      } else {
//        LOG(ERROR) << "connect failed";
//        _good = false;
//        _connected = false;
//        _eof = true;
//      }
//    });
//  }


//  void tds_consumer::handle_fetch_cb(int ec, std::shared_ptr<PGresult> result) {
//    if (ec)
//      return;
//    int tuples_in_batch = PQntuples(result.get());
//    _msg_cnt += tuples_in_batch;
//
//    // should this be here?? it costs something
//    size_t nFields = PQnfields(result.get());
//    for (int i = 0; i < tuples_in_batch; i++)
//      for (int j = 0; j < nFields; j++)
//        _msg_bytes += PQgetlength(result.get(), i, j);
//
//    if (tuples_in_batch == 0) {
//      DLOG(INFO) << "query done, got total: " << _msg_cnt; // tbd remove this
//      _connection->exec("CLOSE MYCURSOR; COMMIT", [](int ec, std::shared_ptr<PGresult> res) {});
//      _eof = true;
//      return;
//    } else {
//      DLOG(INFO) << "query batch done, got total: " << _msg_cnt; // tbd remove this
//      _queue.push_back(result);
//      _connection->exec("FETCH " + std::to_string(_max_items_in_fetch) + " IN MYCURSOR",
//                        [this](int ec, std::shared_ptr<PGresult> res) {
//                          this->handle_fetch_cb(ec, std::move(res));
//                        });
//    }
//  }
//
  void tds_consumer::_thread() {
    while (true) {
      std::this_thread::sleep_for(std::chrono::milliseconds(1000));

      if (_closed)
        break;

      // connected
      if (!_connected) {
        continue;
      }

      std::string fields = "*";
      std::string order_by = "";
      if (_ts_column.size())
        order_by = _ts_column + " ASC, " + _id_column + " ASC";
      else
        order_by = _id_column + " ASC";

      std::string where_clause;

      if (_ts_column.size())
        where_clause = _ts_column;
      else
        where_clause = _id_column;

      //std::string statement = "DECLARE MYCURSOR CURSOR FOR SELECT " + fields + " FROM " + _table + " ORDER BY " + order_by;

      std::string statement = "SELECT " + fields + " FROM " + _table + " ORDER BY " + order_by;

      DLOG(INFO) << "exec(" + statement + ")";

      auto res = _connection->exec(statement);
      if (res.first)
        LOG(FATAL) << "exec failed";


      while (dbnextrow(res.second) != NO_MORE_ROWS) {
        std::cerr << ".";
      }

      int src_numcols;
      if (NO_MORE_RESULTS != dbresults(res.second)) {
        if (0 == (src_numcols = dbnumcols(res.second))) {
          LOG(ERROR) << "Error in dbnumcols";
        }
      }


      //handle_fetch();

      bool more_data = true;

      while (more_data) {
        //handle_fetch();
      }

      //close cursor



      res = _connection->exec(statement);
      if (res.first) {
        LOG(FATAL) << "statement failed " << statement;
      }


      res = _connection->exec("FETCH " + std::to_string(_max_items_in_fetch) + " IN MYCURSOR");

      if (res.first) {
        LOG(FATAL) << "FETCH failed ";
      }

      // handle data
    }
  }



}

