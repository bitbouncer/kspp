#include <kspp/connect/tds/tds_generic_avro_source.h>
//#include <kspp/connect/tds/tds_avro_utils.h>
#include <glog/logging.h>

using namespace std::chrono_literals;

namespace kspp {
  tds_generic_avro_source::tds_generic_avro_source(topology &t,
                                                   int32_t partition,
                                                   std::string table,
                                                   const kspp::connect::connection_params& cp,
                                                   std::string id_column,
                                                   std::string ts_column,
                                                   std::shared_ptr<kspp::avro_schema_registry> registry,
                                                   std::chrono::seconds poll_intervall)
      : partition_source<void, kspp::GenericAvro>(nullptr, partition), _started(false), _exit(false),
        _impl(partition, table, t.consumer_group(), cp, id_column, ts_column, registry, poll_intervall), _schema_registry(registry),
        _schema_id(-1), _commit_chain(table, partition), _parse_errors("parse_errors", "err"),
        _commit_chain_size("commit_chain_size", metric::GAUGE, "msg", [this]() { return _commit_chain.size(); }) {
    this->add_metric(&_commit_chain_size);
    this->add_metrics_tag(KSPP_PROCESSOR_TYPE_TAG, PROCESSOR_NAME);
    this->add_metrics_tag(KSPP_TOPIC_TAG, table);
    this->add_metrics_tag(KSPP_PARTITION_TAG, std::to_string(partition));
  }
}
