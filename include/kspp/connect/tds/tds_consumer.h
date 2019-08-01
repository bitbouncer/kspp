#include <chrono>
#include <memory>
#include <kspp/impl/queue.h>
#include <kspp/topology.h>
#include <kspp/avro/generic_avro.h>
#include <kspp/connect/tds/tds_connection.h>
#include <kspp/connect/tds/tds_read_cursor.h>
#include <kspp/utils/offset_storage_provider.h>
#pragma once

namespace kspp {
  class tds_consumer {
  public:
    tds_consumer(int32_t partition,
                 std::string logical_name,
                 std::string consumer_group,
                 const kspp::connect::connection_params& cp,
                 kspp::connect::table_params tp,
                 std::string query,
                 std::string id_column,
                 std::string ts_column,
                 std::shared_ptr<kspp::avro_schema_registry>);

    ~tds_consumer();

    bool initialize();

    void close();

    inline bool eof() const {
       return (_incomming_msg.size() == 0) && _eof;
    }

    inline std::string logical_name() const {
      return _logical_name;
    }

    inline int32_t partition() const {
      return _partition;
    }

    void start(int64_t offset);

    void subscribe();

    bool is_query_running() const { return !_eof; }

    inline event_queue<kspp::generic_avro, kspp::generic_avro>& queue(){
      return _incomming_msg;
    };

    inline const event_queue<kspp::generic_avro, kspp::generic_avro>& queue() const {
      return _incomming_msg;
    };

    //void commit(bool flush);

    void commit(bool flush) {
      int64_t offset = _commit_chain.last_good_offset();
      if (offset>0 && _offset_storage)
        _offset_storage->commit(offset, flush);
    }


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
    int64_t parse_id(DBPROCESS *stream);

    static void load_avro_by_name(kspp::generic_avro* avro, DBPROCESS *stream, COL *columns); // COL should go away

    int parse_row(DBPROCESS* stream, COL* columns);
    int parse_response(DBPROCESS* stream);
    void commit(int64_t ticks, bool flush);
    void _thread();

    bool _exit;
    bool _start_running;
    bool _good;
    bool _eof;
    bool _closed;

    std::thread _bg;
    std::unique_ptr<kspp_tds::connection> _connection;

    const std::string _logical_name;
    const int32_t _partition;
    const std::string _consumer_group;

    std::shared_ptr<offset_storage> _offset_storage;
    commit_chain _commit_chain;

    const kspp::connect::connection_params _cp;
    const kspp::connect::table_params _tp;

    std::string _query;
    tds_read_cursor _read_cursor;

    const std::string _id_column;

    std::shared_ptr<kspp::avro_schema_registry> _schema_registry;
    std::shared_ptr<avro::ValidSchema> _val_schema;
    std::shared_ptr<avro::ValidSchema> _key_schema;
    std::unique_ptr<kspp::generic_avro> _last_key;
    int32_t _key_schema_id;
    int32_t _val_schema_id;
    event_queue<kspp::generic_avro, kspp::generic_avro> _incomming_msg;

    uint64_t _msg_cnt; // TODO move to metrics
  };
}

