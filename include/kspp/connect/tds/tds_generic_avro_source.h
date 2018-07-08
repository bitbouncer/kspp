#include <memory>
#include <strstream>
#include <thread>
#include <glog/logging.h>
#include <kspp/kspp.h>
#include <kspp/topology.h>
#include <kspp/connect/tds/tds_consumer.h>
//#include <kspp/avro/generic_avro.h>

#pragma once

namespace kspp {
  class tds_generic_avro_source : public partition_source<kspp::generic_avro, kspp::generic_avro> {
    static constexpr const char *PROCESSOR_NAME = "tds_avro_source";
  public:
    tds_generic_avro_source(topology &t,
                            int32_t partition,
                            std::string logical_name,
                            const kspp::connect::connection_params& cp,
                            std::string query,
                            std::string id_column,
                            std::string ts_column,
                            std::shared_ptr<kspp::avro_schema_registry>,
                            std::chrono::seconds poll_intervall = std::chrono::seconds(60));

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
      /*
       * if (_commit_chain.last_good_offset() >= 0 && _impl.commited() < _commit_chain.last_good_offset())
        _impl.commit(_commit_chain.last_good_offset(), true);
        */
      _impl.close();
    }

    bool eof() const override {
      return _impl.eof();
    }

    void commit(bool flush) override {
      /*
       * if (_commit_chain.last_good_offset() >= 0)
        _impl.commit(_commit_chain.last_good_offset(), flush);
        */
    }

    // TBD if we store last offset and end of stream offset we can use this...
    size_t queue_size() const override {
      return _impl.queue().size();
    }

    int64_t next_event_time() const override {
      return _impl.queue().next_event_time();
    }

    size_t process(int64_t tick) override {
      if (_impl.queue().size() == 0)
        return 0;
      size_t processed = 0;
      while (!_impl.queue().empty()) {
        auto p = _impl.queue().front();
        if (p == nullptr || p->event_time() > tick)
          return processed;
        _impl.queue().pop_front();
        this->send_to_sinks(p);
        ++(this->_processed_count);
        ++processed;
        this->_lag.add_event_time(tick, p->event_time());
      }
      return processed;
    }

    std::string topic() const override {
      return _impl.logcal_name();
    }

  protected:
    bool _started;
    bool _exit;
    tds_consumer _impl;
    std::shared_ptr<kspp::avro_schema_registry> _schema_registry;
    std::shared_ptr<avro::ValidSchema> _schema;
    int32_t _schema_id;
    commit_chain _commit_chain;
    metric_counter _parse_errors;
    metric_evaluator _commit_chain_size;
  };
}

