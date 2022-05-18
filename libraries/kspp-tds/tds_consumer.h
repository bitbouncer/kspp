#include <chrono>
#include <memory>
#include <kspp/internal/queue.h>
#include <kspp/topology.h>
#include <kspp/avro/generic_avro.h>
#include <kspp/utils/offset_storage_provider.h>
#include <kspp/internal/commit_chain.h>
#include <kspp-tds/tds_read_cursor.h>
#include <kspp-tds/tds_connection.h>

#pragma once

namespace kspp {
  class tds_consumer {
  public:
    tds_consumer(int32_t partition,
                 std::string logical_name,
                 const kspp::connect::connection_params &cp,
                 kspp::connect::table_params tp,
                 std::string query,
                 std::string id_column,
                 std::string ts_column,
                 std::shared_ptr<kspp::schema_registry_client>);

    ~tds_consumer();

    bool initialize();

    void close();

    inline bool eof() const {
      return (incomming_msg_.size() == 0) && eof_;
    }

    inline std::string logical_name() const {
      return logical_name_;
    }

    inline int32_t partition() const {
      return partition_;
    }

    void start(int64_t offset);

    bool is_query_running() const { return !eof_; }

    inline event_queue<kspp::generic_avro, kspp::generic_avro> &queue() {
      return incomming_msg_;
    };

    inline const event_queue<kspp::generic_avro, kspp::generic_avro> &queue() const {
      return incomming_msg_;
    };

    void commit(bool flush) {
      int64_t offset = commit_chain_.last_good_offset();
      if (offset > 0)
        offset_storage_->commit(offset, flush);
    }

  private:
    // this should go away when we can parse datetime2
    struct COL {
      char *name;
      char *buffer;
      int type;
      int size;
      int status;
    };

    void connect_async();

    int64_t parse_ts(DBPROCESS *stream);

    int64_t parse_id(DBPROCESS *stream);

    static void load_avro_by_name(kspp::generic_avro *avro, DBPROCESS *stream, COL *columns); // COL should go away

    int parse_row(DBPROCESS *stream, COL *columns);

    int parse_response(DBPROCESS *stream);

    void commit(int64_t ticks, bool flush);

    void _thread();


    std::unique_ptr<kspp_tds::connection> connection_;
    const kspp::connect::connection_params cp_;
    const kspp::connect::table_params tp_;
    std::string query_;
    const std::string logical_name_;
    const int32_t partition_;
    const std::string id_column_;
    std::shared_ptr<kspp::schema_registry_client> schema_registry_;

    bool exit_ = false;
    bool start_running_ = false;
    bool eof_ = false;
    bool closed_ = false;

    std::shared_ptr<offset_storage> offset_storage_;
    commit_chain commit_chain_;
    tds_read_cursor read_cursor_;


    std::shared_ptr<avro::ValidSchema> val_schema_;
    std::shared_ptr<avro::ValidSchema> key_schema_;
    std::unique_ptr<kspp::generic_avro> last_key_;
    int32_t key_schema_id_ = -1;
    int32_t val_schema_id_ = -1;
    event_queue<kspp::generic_avro, kspp::generic_avro> incomming_msg_;
    uint64_t msg_cnt_ = 0; // TODO move to metrics

    std::thread bg_;
  };
}

