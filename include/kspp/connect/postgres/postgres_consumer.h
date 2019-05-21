#include <chrono>
#include <memory>
#include <kspp/impl/queue.h>
#include <kspp/topology.h>
#include <kspp/avro/generic_avro.h>
#include <kspp/connect/postgres/postgres_connection.h>
#pragma once

namespace kspp {
  class postgres_consumer {
  public:
    postgres_consumer(int32_t partition,
                       std::string locical_name,
                       std::string consumer_group,
                       const kspp::connect::connection_params& cp,
                       kspp::connect::table_params tp,
                       std::string query,
                       std::string id_column,
                       std::string ts_column,
                       std::shared_ptr<kspp::avro_schema_registry>);

    ~postgres_consumer();

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

  private:
    void connect();
    int parse_response(std::shared_ptr<PGresult>);
    std::string get_where_clause() const;

    void _thread();
    bool _exit;
    bool _start_running;
    bool _good;
    bool _eof;
    bool _closed;
    std::thread _bg;
    std::unique_ptr<kspp_postgres::connection> _connection;
    const std::string _logical_name;
    const std::string _query;
    const int32_t _partition;
    const std::string _consumer_group;
    const kspp::connect::connection_params cp_;
    const kspp::connect::table_params tp_;
    const std::string _id_column;
    const std::string _ts_column;
    // this holds the read cursor (should be abstracted)
    int id_column_index_;
    int ts_column_index_;
    std::string last_id_;
    std::string last_ts_;

    std::shared_ptr<kspp::avro_schema_registry> schema_registry_;
    std::shared_ptr<avro::ValidSchema> key_schema_;
    std::unique_ptr<kspp::generic_avro> last_key_;
    std::shared_ptr<avro::ValidSchema> value_schema_;
    int32_t key_schema_id_;
    int32_t value_schema_id_;
    event_queue<kspp::generic_avro, kspp::generic_avro> _incomming_msg;
    // move
    uint64_t _msg_cnt; // TODO move to metrics
  };
}

