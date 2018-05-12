#include <kspp/connect/tds/tds_consumer.h>
#include <kspp/kspp.h>
#include <chrono>
#include <memory>
#include <glog/logging.h>
#include <boost/bind.hpp>

namespace kspp {

  struct COL
  {
    char *name;
    char *buffer;
    int type;
    int    size;
    int status;
  };


  typedef enum
  {
    SYBCHAR = 47,		/* 0x2F */
    SYBVARCHAR = 39,	/* 0x27 */
    SYBINTN = 38,		/* 0x26 */
    SYBINT1 = 48,		/* 0x30 */
    SYBINT2 = 52,		/* 0x34 */
    SYBINT4 = 56,		/* 0x38 */
    SYBFLT8 = 62,		/* 0x3E */
    SYBDATETIME = 61,	/* 0x3D */
    SYBBIT = 50,		/* 0x32 */
    SYBTEXT = 35,		/* 0x23 */
    SYBNTEXT = 99,		/* 0x63 */
    SYBIMAGE = 34,		/* 0x22 */
    SYBMONEY4 = 122,	/* 0x7A */
    SYBMONEY = 60,		/* 0x3C */
    SYBDATETIME4 = 58,	/* 0x3A */
    SYBREAL = 59,		/* 0x3B */
    SYBBINARY = 45,		/* 0x2D */
    SYBVOID = 31,		/* 0x1F */
    SYBVARBINARY = 37,	/* 0x25 */
    SYBBITN = 104,		/* 0x68 */
    SYBNUMERIC = 108,	/* 0x6C */
    SYBDECIMAL = 106,	/* 0x6A */
    SYBFLTN = 109,		/* 0x6D */
    SYBMONEYN = 110,	/* 0x6E */
    SYBDATETIMN = 111,	/* 0x6F */

/*
 * MS only types
 */
        SYBNVARCHAR = 103,	/* 0x67 */
    SYBINT8 = 127,		/* 0x7F */
    XSYBCHAR = 175,		/* 0xAF */
    XSYBVARCHAR = 167,	/* 0xA7 */
    XSYBNVARCHAR = 231,	/* 0xE7 */
    XSYBNCHAR = 239,	/* 0xEF */
    XSYBVARBINARY = 165,	/* 0xA5 */
    XSYBBINARY = 173,	/* 0xAD */
    SYBUNIQUE = 36,		/* 0x24 */
    SYBVARIANT = 98, 	/* 0x62 */
    SYBMSUDT = 240,		/* 0xF0 */
    SYBMSXML = 241,		/* 0xF1 */
    SYBMSDATE = 40,  	/* 0x28 */
    SYBMSTIME = 41,  	/* 0x29 */
    SYBMSDATETIME2 = 42,  	/* 0x2a */
    SYBMSDATETIMEOFFSET = 43,/* 0x2b */

/*
 * Sybase only types
 */
        SYBLONGBINARY = 225,	/* 0xE1 */
    SYBUINT1 = 64,		/* 0x40 */
    SYBUINT2 = 65,		/* 0x41 */
    SYBUINT4 = 66,		/* 0x42 */
    SYBUINT8 = 67,		/* 0x43 */
    SYBBLOB = 36,		/* 0x24 */
    SYBBOUNDARY = 104,	/* 0x68 */
    SYBDATE = 49,		/* 0x31 */
    SYBDATEN = 123,		/* 0x7B */
    SYB5INT8 = 191,		/* 0xBF */
    SYBINTERVAL = 46,	/* 0x2E */
    SYBLONGCHAR = 175,	/* 0xAF */
    SYBSENSITIVITY = 103,	/* 0x67 */
    SYBSINT1 = 176,		/* 0xB0 */
    SYBTIME = 51,		/* 0x33 */
    SYBTIMEN = 147,		/* 0x93 */
    SYBUINTN = 68,		/* 0x44 */
    SYBUNITEXT = 174,	/* 0xAE */
    SYBXML = 163,		/* 0xA3 */
    SYB5BIGDATETIME = 187,	/* 0xBB */
    SYB5BIGTIME = 188,	/* 0xBC */

  } TDS_SERVER_TYPE;

  void print_data(std::string t, int column_type, BYTE* data, int len){
    if (data==nullptr) {
      std::cerr << t << ": NULL" << std::endl;
      return;
    }

    switch (column_type)
    {
      case SYBINT2:
      {
        int16_t v;
        assert(sizeof(v)==len);
        memcpy(&v, data, sizeof(v));
        std::cerr << t << ": " << v << std::endl;
      }
      break;

      case SYBINT4:
      {
        int32_t v;
        assert(sizeof(v)==len);
        memcpy(&v, data, sizeof(v));
        std::cerr << t << ": " << v << std::endl;
      }
      break;

      case SYBINT8:
      {
        int64_t v;
        assert(sizeof(v)==len);
        memcpy(&v, data, sizeof(v));
        std::cerr << t << ": " << v << std::endl;
      }
      break;

      case SYBFLT8:
      {
        double v;
        assert(sizeof(v)==len);
        memcpy(&v, data, sizeof(v));
        std::cerr << t << ": " << v << std::endl;
      }
      break;

      case SYBCHAR:
      {
        std::string s;
        s.reserve(len);
        s.assign((const char*) data, len);
        std::cerr << t << ": " << s << std::endl;
      }
      break;

      case SYBMSUDT:
      {
        std::string s;
        s.reserve(len);
        s.assign((const char*) data, len);
        std::cerr << t << ": " << s << std::endl;
      }
      break;

      /*
       * case SYBDATETIME:
      case SYBDATETIME4:
      case SYBMSTIME:
      case SYBMSDATE:
      case SYBMSDATETIMEOFFSET:
      case SYBTIME:
      case SYBDATE:
      case SYB5BIGTIME:
      case SYB5BIGDATETIME:
       */
      case SYBMSDATETIME2:
      {
        std::string s;
        s.reserve(len);
        s.assign((const char*) data, len);
        std::cerr << t << ": " << s << std::endl;
      }
      break;

     default:
      std::cerr << t << " " << column_type << " size " << len << " not implemented"<< std::endl;
    }

  }

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
      DLOG(INFO) << "tds_consumer::start table:" << _table << ":" << _partition  << " consumer group: " << _consumer_group << " starting from OFFSET_BEGINNING";
    } else if (offset == kspp::OFFSET_END) {
      DLOG(INFO) << "tds_consumer::start table:" << _table << ":" << _partition  << " consumer group: " << _consumer_group << " starting from OFFSET_END";
    } else {
      DLOG(INFO) << "tds_consumer::start table:" << _table << ":" << _partition  << " consumer group: " << _consumer_group << " starting from fixed offset: " << offset;
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


      if (dbhasretstat(res.second) == TRUE) {
        LOG(INFO) << "Procedure returned: " << dbretstatus(res.second);
      } else {
        LOG(INFO) << "Procedure returned: none";
      }

      int ncomputeids = dbnumcompute(res.second);

      /*while (dbnextrow(res.second) != NO_MORE_ROWS) {
        std::cerr << ".";
      }
       */


      RETCODE erc;
      while ((erc = dbresults(res.second)) != NO_MORE_RESULTS) {


        if (erc == FAIL) {
          LOG(FATAL) << "dbresults failed";
        }

        COL *columns = nullptr;

        auto ncols = dbnumcols(res.second);

        if ((columns = (COL *) calloc(ncols, sizeof(struct COL))) == NULL) {
          perror(NULL);
          exit(1);
        }


/*
			 * Read metadata and bind.
			 */

        for (COL *pcol = columns; pcol - columns < ncols; pcol++) {
          int c = pcol - columns + 1;

          pcol->name = dbcolname(res.second, c);
          pcol->type = dbcoltype(res.second, c);
          //pcol->size = dbcollen(res.second, c);
          pcol->size=0; // AUTOCONV SIZE

          /*if (SYBCHAR != pcol->type) {
            pcol->size = dbprcollen(res.second, c);
            if (pcol->size > 255)
              pcol->size = 255;
          }
           */

          if (pcol->type==SYBMSDATETIME2) {
            pcol->size = 64; //test
          }

          //if (pcol->type==SYBMSUDT) {
          //  pcol->size = 64; //test
          //}

          //printf("%*s ", pcol->size, pcol->name);
          if (pcol->size>0) {

            if ((pcol->buffer = (char *) calloc(1, pcol->size + 1)) == NULL) {
              perror(NULL);
              exit(1);
            }

            auto erc = dbbind(res.second, c, NTBSTRINGBIND,
                              pcol->size + 1, (BYTE *) pcol->buffer);
            if (erc == FAIL) {
              LOG(FATAL) << "dbbind( " << c << ") failed";
            }

            erc = dbnullbind(res.second, c, &pcol->status);
            if (erc == FAIL) {
              LOG(FATAL) << "dbnullbind( " << c << ") failed";

            }
          }

        }


        int row_code = 0;


        /*
         * while ((row_code = dbnextrow(res.second)) != NO_MORE_ROWS) {
          switch (row_code) {
            case REG_ROW:
              for (COL *pcol = columns; pcol - columns < ncols; pcol++) {
                const char *buffer = (pcol->status == -1) ? "NULL" : pcol->buffer;
                //std::cerr << std::string(buffer, pcol->size) << std::endl;
                std::cerr << std::string(buffer) << "\t";

                //printf("%*s ", pcol->size, buffer);
              }

              break;

            case BUF_FULL:
              assert(false);
              break;

            case FAIL:
              LOG(FATAL) << "dbresults failed";
              break;

            default:
              LOG(INFO) << "Data for computeid " << row_code << " ignored";
          }
          std::cerr << std::endl;
          //dbclrbuf(res.second, 1); // lear oldest line
        }
         */


        while ((row_code = dbnextrow(res.second)) != NO_MORE_ROWS) {
          switch (row_code) {
            case REG_ROW: {
              COL *pcol = columns;
              for (auto i = 0; i != ncols; ++i, pcol++) {
                if (pcol->buffer)
                {
                  const char *buffer = (pcol->status == -1) ? "NULL" : pcol->buffer;
                  std::cerr << dbcolname(res.second, i + 1) << ": " << buffer << std::endl;

                } else {
                  print_data(dbcolname(res.second, i + 1), dbcoltype(res.second, i + 1), dbdata(res.second, i + 1),
                             dbdatlen(res.second, i + 1));
                }
                }
              /*
               * for (COL *pcol = columns; pcol - columns < ncols; pcol++) {
                const char *buffer = (pcol->status == -1) ? "NULL" : pcol->buffer;
                //std::cerr << std::string(buffer, pcol->size) << std::endl;
                std::cerr << std::string(buffer) << "\t";

                //printf("%*s ", pcol->size, buffer);
              }
               */
            }
              break;

            case BUF_FULL:
              assert(false);
              break;

            case FAIL:
              LOG(FATAL) << "dbresults failed";
              break;

            default:
              LOG(INFO) << "Data for computeid " << row_code << " ignored";
          }
          std::cerr << std::endl;
          //dbclrbuf(res.second, 1); // lear oldest line
        }
      }



//      int ncols=0;
//      if (NO_MORE_RESULTS != dbresults(res.second)) {
//        if (0 == (ncols = dbnumcols(res.second))) {
//          LOG(ERROR) << "Error in dbnumcols";
//        }
//      }


      /*
       * for (int c=0; c < ncols; c++) {
        /* Get and print the metadata.  Optional: get only what you need. */
        //LOG(INFO) << dbcolname(res.second, c + 1) << ", type: " << dbcoltype(res.second, c + 1) << "size: "
        //          << dbcollen(res.second, c + 1);
      //}

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

