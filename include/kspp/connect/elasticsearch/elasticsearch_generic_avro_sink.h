#include <kspp/connect/generic_avro_sink.h>
#include <kspp/connect/elasticsearch/elasticsearch_producer.h>
#pragma once

namespace kspp {
  class elasticsearch_generic_avro_sink : public generic_avro_sink {
    static constexpr const char *PROCESSOR_NAME = "elasticsearch_avro_sink";
  public:
    elasticsearch_generic_avro_sink(std::shared_ptr<cluster_config> config, const kspp::connect::connection_params& cp, size_t max_http_connection=20)
        : generic_avro_sink(config, std::make_shared<kspp::elasticsearch_producer>(cp, max_http_connection)) {
      this->add_metrics_tag(KSPP_PROCESSOR_TYPE_TAG, PROCESSOR_NAME);
      this->add_metrics_tag(KSPP_TOPIC_TAG, cp.database_name);

      // register sub component metrics
      this->register_metrics(this);
    }

    bool good() const override {
      return this->good();
    }

    std::string log_name() const override {
      return PROCESSOR_NAME;
    }
  };
}
