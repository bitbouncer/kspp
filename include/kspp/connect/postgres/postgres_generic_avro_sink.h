#include <memory>
#include <strstream>
#include <thread>
#include <glog/logging.h>
#include <kspp/kspp.h>
#include <kspp/topology.h>

#include <kspp/connect/generic_avro_sink.h>
#include <kspp/connect/postgres/postgres_producer.h>

#include <kspp/avro/avro_generic.h>
#pragma once

namespace kspp {
  class postgres_generic_avro_sink : public generic_avro_sink {
    static constexpr const char* PROCESSOR_NAME = "postgres_avro_sink";
  public:
    postgres_generic_avro_sink(topology &t,
                               std::string table,
                               std::string connect_string,
                               std::string id_column,
                               std::shared_ptr<kspp::avro_schema_registry> schema_registry,
                               std::string client_encoding="UTF8")
        : generic_avro_sink(t, std::make_shared<kspp::postgres_producer>(table, connect_string, id_column, client_encoding), schema_registry){
      this->add_metrics_tag(KSPP_PROCESSOR_TYPE_TAG, PROCESSOR_NAME);
      this->add_metrics_tag(KSPP_TOPIC_TAG, table);
    }

    std::string log_name() const override {
      return PROCESSOR_NAME;
    }


  protected:
    std::shared_ptr<kspp::avro_schema_registry> _schema_registry;
    std::shared_ptr<avro::ValidSchema> _schema;
    int32_t _schema_id;
  };
}

