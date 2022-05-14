#include <kspp-tds/tds_generic_avro_source.h>
#include <glog/logging.h>

using namespace std::chrono_literals;

namespace kspp {
  tds_generic_avro_source::tds_generic_avro_source(std::shared_ptr<cluster_config> config,
                                                   int32_t partition,
                                                   std::string logical_name,
                                                   const kspp::connect::connection_params &cp,
                                                   kspp::connect::table_params tp,
                                                   std::string query,
                                                   std::string id_column,
                                                   std::string ts_column,
                                                   std::shared_ptr<kspp::avro_schema_registry> registry)
      : partition_source<kspp::generic_avro, kspp::generic_avro>(nullptr, partition),
        impl_(partition, logical_name, cp, tp, query, id_column, ts_column, registry) {
    this->add_metrics_label(KSPP_PROCESSOR_TYPE_TAG, PROCESSOR_NAME);
    this->add_metrics_label(KSPP_TOPIC_TAG, logical_name);
    this->add_metrics_label(KSPP_PARTITION_TAG, std::to_string(partition));
  }
}
