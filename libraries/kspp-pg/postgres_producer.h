#include <chrono>
#include <memory>
#include <kspp/internal/queue.h>
#include <kspp/topology.h>
#include <kspp/connect/generic_producer.h>
#include <kspp-pg/postgres_connection.h>

#pragma once

namespace kspp {
  class postgres_producer : public generic_producer<kspp::generic_avro, kspp::generic_avro> {
  public:
    enum {
      MAX_ERROR_BEFORE_BAD = 200
    };

    postgres_producer(std::string table,
                      const kspp::connect::connection_params &cp,
                      std::vector<std::string> keys,
                      std::string client_encoding,
                      size_t max_items_in_insert,
                      bool skip_delete = false);

    ~postgres_producer();

    void register_metrics(kspp::processor *parent) override;

    void close() override;

    bool good() const {
      return (current_error_streak_ < MAX_ERROR_BEFORE_BAD);
    }

    bool eof() const override {
      return (incomming_msg_.empty() && done_.empty());
    }

    std::string topic() const override {
      return table_;
    }

    void stop();

    void insert(std::shared_ptr<kevent<kspp::generic_avro, kspp::generic_avro>> p) override {
      incomming_msg_.push_back(p);
    }

    void poll() override;

    size_t queue_size() const override {
      auto sz0 = incomming_msg_.size();
      auto sz1 = done_.size();
      return sz0 + sz1;
    }

  private:
    bool initialize();

    bool check_table_exists();

    void _thread();

    bool exit_=false;
    bool start_running_=false;
    bool closed_=false;
    std::unique_ptr<kspp_postgres::connection> connection_;
    const std::string table_;
    const kspp::connect::connection_params cp_;
    const std::vector<std::string> id_columns_;
    const std::string client_encoding_;
    event_queue<kspp::generic_avro, kspp::generic_avro> incomming_msg_;
    event_queue<kspp::generic_avro, kspp::generic_avro> done_;  // waiting to be deleted in poll();
    const size_t max_items_in_insert_;
    bool table_checked_ = false;
    bool table_exists_ = false;
    const bool skip_delete2_;
    size_t current_error_streak_=0;
    metric_counter connection_errors_;
    metric_counter insert_errors_;
    metric_counter msg_cnt_;
    metric_counter msg_bytes_;
    metric_summary request_time_;
    std::thread bg_;
  };
}

