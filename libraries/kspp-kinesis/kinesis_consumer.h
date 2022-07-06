#ifdef KSPP_KINESIS
#include <chrono>
#include <memory>
#include <aws/core/Aws.h>
#include <aws/kinesis/KinesisClient.h>
#include <kspp/internal/queue.h>
#include <kspp/internal/commit_chain.h>
#include <kspp/topology.h>
#include <kspp/utils/offset_storage_provider.h>
#pragma once

namespace kspp {
  class kinesis_consumer {
  public:
    kinesis_consumer(int32_t partition, std::string stream_name);

    ~kinesis_consumer();

    bool initialize();

    void close();

    inline bool eof() const {
      return (incomming_msg_.size() == 0) && eof_;
    }

    inline std::string logical_name() const {
      return stream_name_;
    }

    inline int32_t partition() const {
      return partition_;
    }

    void start(int64_t offset);

    //void subscribe();

    inline event_queue<std::string, std::string>& queue(){
      return incomming_msg_;
    };

    inline const event_queue<std::string, std::string>& queue() const {
      return incomming_msg_;
    };

    void commit(bool flush) {
      int64_t offset = commit_chain_.last_good_offset();
      if (offset>0)
        offset_storage_->commit(offset, flush);
    }

  private:
    //int parse_response(std::shared_ptr<PGresult>);
    void _thread();
    bool exit_=false;
    bool start_running_=false;
    bool eof_=false;
    bool closed_=false;
    std::thread bg_;
    std::unique_ptr<Aws::Kinesis::KinesisClient> client_;
    Aws::String shard_iterator_;

    //postgres_read_cursor read_cursor_;
    commit_chain commit_chain_;
    const int32_t partition_;
    const std::string stream_name_;
    std::shared_ptr<offset_storage> offset_storage_;
    event_queue<std::string, std::string> incomming_msg_;
    // move
    uint64_t msg_cnt_=0; // TODO move to metrics
  };
}
#endif
