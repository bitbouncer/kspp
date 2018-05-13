#include <chrono>
#include <memory>
#include <kspp/impl/queue.h>
#include <kspp/connect/tds/tds_connection.h>
#include <kspp/topology.h>
#include <kspp/avro/avro_generic.h>

#pragma once

namespace kspp {
  class tds_consumer {
  public:
    tds_consumer(int32_t partition,
                 std::string table,
                 std::string consumer_group,
                 std::string host,
                 std::string user,
                 std::string password,
                 std::string database,
                 std::string id_column,
                 std::string ts_column,
                 std::shared_ptr<kspp::avro_schema_registry>,
                 std::chrono::seconds poll_intervall);

    ~tds_consumer();

    bool initialize();

    void close();

    //std::unique_ptr<RdKafka::Message> consume();
    //std::shared_ptr<PGresult> consume();

    inline bool eof() const {
      bool eof = (_incomming_msg.size() == 0) && _eof;
      if (eof)
        std::cerr << "eof()" << std::endl;
       return (_incomming_msg.size() == 0) && _eof;
    }

    inline std::string table() const {
      return _table;
    }

    inline int32_t partition() const {
      return _partition;
    }

    void start(int64_t offset);

    //void stop();

    //int32_t commit(int64_t offset, bool flush = false);

    //inline int64_t commited() const {
    //  return _can_be_committed;
    //}

    void subscribe();

    bool is_connected() const { return _connected; }

    bool is_query_running() const { return !_eof; }

    inline event_queue<void, kspp::GenericAvro>& queue(){
      return _incomming_msg;
    };

    inline const event_queue<void, kspp::GenericAvro>& queue() const {
      return _incomming_msg;
    };

  private:
    // this should go away when we can parse datetime2
    struct COL
    {
      char *name;
      char *buffer;
      int type;
      int size;
      int status;
    };

    void connect_async();
    int64_t parse_ts(DBPROCESS *stream);
    int parse_avro(DBPROCESS* stream, COL* columns, size_t ncols);
    int parse_response(DBPROCESS* stream);
    void _thread();

    bool _exit;
    bool _good;
    bool _connected;
    bool _eof;
    bool _closed;
    std::chrono::seconds poll_intervall_;

    std::thread _bg;
    std::shared_ptr<kspp_tds::connection> _connection;

    const std::string _table;
    const int32_t _partition;
    const std::string _consumer_group;

    const std::string host_;
    const std::string user_;
    const std::string password_;
    const std::string database_;

    const std::string _id_column;
    const std::string _ts_column;

    int ts_column_index_;
    int64_t last_ts_;

    std::shared_ptr<kspp::avro_schema_registry> schema_registry_;
    std::shared_ptr<avro::ValidSchema> schema_;
    int32_t schema_id_;
    event_queue<void, kspp::GenericAvro> _incomming_msg;

    uint64_t _msg_cnt;
  };
}

