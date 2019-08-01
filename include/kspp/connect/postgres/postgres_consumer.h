#include <chrono>
#include <memory>
#include <kspp/impl/queue.h>
#include <kspp/topology.h>
#include <kspp/avro/generic_avro.h>
#include <kspp/utils/offset_storage_provider.h>
#include <kspp/connect/postgres/postgres_connection.h>
#include <kspp/connect/postgres/postgres_read_cursor.h>
#pragma once

namespace kspp {
  class postgres_consumer {
  public:
    postgres_consumer(int32_t partition,
                       std::string locical_name,
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
      return (_incomming_msg.size() == 0) && eof_;
    }

    inline std::string logical_name() const {
      return logical_name_;
    }

    inline int32_t partition() const {
      return partition_;
    }

    void start(int64_t offset);

    void subscribe();

    inline event_queue<kspp::generic_avro, kspp::generic_avro>& queue(){
      return _incomming_msg;
    };

    inline const event_queue<kspp::generic_avro, kspp::generic_avro>& queue() const {
      return _incomming_msg;
    };

    void commit(bool flush) {
      int64_t offset = commit_chain_.last_good_offset();
      if (offset>0)
        offset_storage_->commit(offset, flush);
    }

  private:
    //void connect();
    void load_oids_for_extensions();
    std::shared_ptr<avro::ValidSchema> schema_for_table_row(std::string schema_name,  const PGresult *res) const;
    int parse_response(std::shared_ptr<PGresult>);
    std::string get_where_clause() const;

    void _thread();
    bool exit_;
    bool start_running_;
    bool eof_;
    bool closed_;
    std::thread bg_;
    std::unique_ptr<kspp_postgres::connection> connection_;
    const std::string logical_name_;
    const std::string query_;
    postgres_read_cursor read_cursor_;
    commit_chain commit_chain_;
    const int32_t partition_;
    std::shared_ptr<offset_storage> offset_storage_;
    const kspp::connect::connection_params cp_;
    const kspp::connect::table_params tp_;

    const std::string id_column_;

    std::shared_ptr<kspp::avro_schema_registry> schema_registry_;
    std::shared_ptr<avro::ValidSchema> key_schema_;
    std::unique_ptr<kspp::generic_avro> last_key_;
    std::shared_ptr<avro::ValidSchema> value_schema_;
    std::map<int, boost::shared_ptr<avro::Schema>> extension_oids_; // currently hstore

    int32_t key_schema_id_;
    int32_t value_schema_id_;
    event_queue<kspp::generic_avro, kspp::generic_avro> _incomming_msg;
    // move
    uint64_t _msg_cnt; // TODO move to metrics
  };
}

