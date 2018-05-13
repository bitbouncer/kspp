#include <kspp/connect/generic_avro_sink.h>
#include <kspp/connect/elasticsearch/elasticsearch_producer.h>
#pragma once

namespace kspp {
  class elasticsearch_generic_avro_sink : public generic_avro_sink {
    static constexpr const char *PROCESSOR_NAME = "elasticsearch_avro_sink";
  public:
    elasticsearch_generic_avro_sink(topology &t,
                                    std::string table,
                                    std::string base_url,
                                    std::string user,
                                    std::string password,
                                    std::string id_column,
                                    std::shared_ptr<kspp::avro_schema_registry> schema_registry)
        : generic_avro_sink(t, std::make_shared<kspp::elasticsearch_producer>(table, base_url, user, password, id_column, 20), schema_registry) {
      this->add_metrics_tag(KSPP_PROCESSOR_TYPE_TAG, PROCESSOR_NAME);
      this->add_metrics_tag(KSPP_TOPIC_TAG, table);
    }

    std::string log_name() const override {
      return PROCESSOR_NAME;
    }
  };
}

