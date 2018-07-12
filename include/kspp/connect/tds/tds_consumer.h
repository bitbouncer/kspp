#include <chrono>
#include <memory>
#include <kspp/impl/queue.h>
#include <kspp/connect/tds/tds_connection.h>
#include <kspp/topology.h>
#include <kspp/avro/generic_avro.h>

#pragma once

namespace kspp {
  class tds_consumer {
  public:
    tds_consumer(int32_t partition,
                 std::string logical_name,
                 std::string consumer_group,
                 const kspp::connect::connection_params& cp,
                 std::string query,
                 std::string id_column,
                 std::string ts_column,
                 std::shared_ptr<kspp::avro_schema_registry>,
                 std::chrono::seconds poll_intervall,
                 size_t max_items_in_fetch=30000);

    ~tds_consumer();

    bool initialize();

    void close();

    inline bool eof() const {
       return (_incomming_msg.size() == 0) && _eof;
    }

    inline std::string logcal_name() const {
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
    void _thread();

    bool _exit;
    bool _start_running;
    bool _good;
    bool _eof;
    bool _closed;
    std::chrono::seconds poll_intervall_;

    std::thread _bg;
    std::shared_ptr<kspp_tds::connection> _connection;

    const std::string _logical_name;
    const int32_t _partition;
    const std::string _consumer_group;

    const kspp::connect::connection_params cp_;

    std::string _query;
    const std::string _id_column;
    const std::string _ts_column;
    size_t _max_items_in_fetch;

    int ts_column_index_;
    int id_column_index_;
    int64_t last_ts_;
    int64_t last_id_;

    std::shared_ptr<kspp::avro_schema_registry> schema_registry_;
    std::shared_ptr<avro::ValidSchema> val_schema_;
    std::shared_ptr<avro::ValidSchema> key_schema_;
    std::unique_ptr<kspp::generic_avro> last_key_;
    int32_t key_schema_id_;
    int32_t val_schema_id_;
    event_queue<kspp::generic_avro, kspp::generic_avro> _incomming_msg;

    uint64_t _msg_cnt;
  };
}

