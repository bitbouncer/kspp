#include <memory>
#include <strstream>
#include <thread>
#include <glog/logging.h>
#include <kspp/kspp.h>
#include <kspp/topology.h>
#include <kspp/connect/tds/tds_consumer.h>
#include <kspp/avro/avro_generic.h>

#pragma once

namespace kspp {
  class tds_generic_avro_source : public partition_source<void, kspp::GenericAvro> {
    static constexpr const char *PROCESSOR_NAME = "tds_avro_source";
  public:
    tds_generic_avro_source(topology &t,
                            int32_t partition,
                            std::string table,
                            std::string host,
                            std::string user,
                            std::string password,
                            std::string database,
                            std::string id_column,
                            std::string ts_column,
                            std::shared_ptr<kspp::avro_schema_registry>);

    virtual ~tds_generic_avro_source() {
      close();
    }

    std::string log_name() const override {
      return PROCESSOR_NAME;
    }

    void start(int64_t offset) override {
      _impl.start(offset);
      _started = true;
    }

    void close() override {
      if (!_exit) {
        _exit = true;
        _thread.join();
      }

      if (_commit_chain.last_good_offset() >= 0 && _impl.commited() < _commit_chain.last_good_offset())
        _impl.commit(_commit_chain.last_good_offset(), true);
      _impl.close();
    }

    bool eof() const override {
      return _incomming_msg.size() == 0 && _impl.eof();
    }

    void commit(bool flush) override {
      if (_commit_chain.last_good_offset() >= 0)
        _impl.commit(_commit_chain.last_good_offset(), flush);
    }

    // TBD if we store last offset and end of stream offset we can use this...
    size_t queue_size() const override {
      return _incomming_msg.size();
    }

    int64_t next_event_time() const override {
      return _incomming_msg.next_event_time();
    }

    size_t process(int64_t tick) override {
      if (_incomming_msg.size() == 0)
        return 0;
      size_t processed = 0;
      while (!_incomming_msg.empty()) {
        auto p = _incomming_msg.front();
        if (p == nullptr || p->event_time() > tick)
          return processed;
        _incomming_msg.pop_front();
        this->send_to_sinks(p);
        ++(this->_processed_count);
        ++processed;
        this->_lag.add_event_time(tick, p->event_time());
      }
      return processed;
    }

    std::string topic() const override {
      return _impl.table();
    }

  protected:
    //void parse(const PGresult* ref);
    void thread_f();

    bool _started;
    bool _exit;
    std::thread _thread;
    event_queue<void, kspp::GenericAvro> _incomming_msg;
    tds_consumer _impl;
    std::shared_ptr<kspp::avro_schema_registry> _schema_registry;
    std::shared_ptr<avro::ValidSchema> _schema;
    int32_t _schema_id;
    commit_chain _commit_chain;
    metric_counter _parse_errors;
    metric_evaluator _commit_chain_size;
  };
}

