#include <memory>
#include <kspp/avro/generic_avro.h>
#include <kspp/kspp.h>
#pragma once

namespace kspp {
class generic_avro_file_source : public partition_source<void, kspp::generic_avro> {
    static constexpr const char *PROCESSOR_NAME = "avro_file_source";
  public:
    generic_avro_file_source(std::shared_ptr<cluster_config> config, int32_t partition, std::string source);

    ~generic_avro_file_source() override;

    std::string log_name() const override {
      return PROCESSOR_NAME;
    }

    void start(int64_t offset) override;

    void commit(bool flush) override;

    void close() override;

    bool eof() const override;

    size_t queue_size() const override;

    int64_t next_event_time() const override;

    size_t process(int64_t tick) override;

    std::string topic() const override;

  protected:
    void thread_f();

    size_t max_incomming_queue_size_ = 1000;
    bool started_ = false;
    bool exit_ = false;
    bool eof_ = false;
    std::thread thread_;
    std::string source_;
    int64_t messages_in_file_ = 0;
    event_queue<void, kspp::generic_avro> incomming_msg_;
  };
}