#include <kspp/connect/postgres/postgres_generic_avro_source.h>
#include <glog/logging.h>

using namespace std::chrono_literals;

namespace kspp {
  postgres_generic_avro_source::postgres_generic_avro_source(std::shared_ptr<cluster_config> config,
                                                               int32_t partition,
                                                               std::string logical_name,
                                                               const kspp::connect::connection_params& cp,
                                                               kspp::connect::table_params tp,
                                                               std::string query,
                                                               std::string id_column,
                                                               std::string ts_column,
                                                               std::shared_ptr<kspp::avro_schema_registry> registry)
      : partition_source<kspp::generic_avro, kspp::generic_avro>(nullptr, partition)
      , _impl(partition, logical_name, config->get_consumer_group(), cp, tp, query, id_column, ts_column, registry){
    this->add_metrics_label(KSPP_PROCESSOR_TYPE_TAG, PROCESSOR_NAME);
    this->add_metrics_label(KSPP_TOPIC_TAG, logical_name);
    this->add_metrics_label(KSPP_PARTITION_TAG, std::to_string(partition));
  }
}
