#include <kspp/connect/generic_avro_sink.h>
#include <kspp/connect/elasticsearch/elasticsearch_producer.h>
#include <kspp/connect/elasticsearch/elasticsearch_utils.h>
#pragma once

namespace kspp {
  class elasticsearch_generic_avro_sink : public generic_avro_sink {
    static constexpr const char *PROCESSOR_NAME = "elasticsearch_avro_sink";
  public:
    elasticsearch_generic_avro_sink(std::shared_ptr<cluster_config> config, const kspp::connect::connection_params& cp, size_t max_http_connection=20)
        : generic_avro_sink(config, std::make_shared<kspp::elasticsearch_producer<generic_avro, generic_avro>>(
            cp,
            [](const generic_avro& key){
              return avro_2_raw_column_value(*key.generic_datum());
              },
            [](const generic_avro& value){
              return avro2elastic_json(*value.valid_schema(), *value.generic_datum());
            },
            max_http_connection)) {
      this->add_metrics_label(KSPP_PROCESSOR_TYPE_TAG, PROCESSOR_NAME);
      this->add_metrics_label(KSPP_TOPIC_TAG, cp.database_name);

      // register sub component metrics
      this->register_metrics(this);
    }

    /*bool good() const override {
      return this->good();
    }*/

    std::string log_name() const override {
      return PROCESSOR_NAME;
    }
  };
}
