#include <kspp/connect/postgres/postgres_generic_avro_source.h>
#include <glog/logging.h>

using namespace std::chrono_literals;

namespace kspp {
  postgres_generic_avro_source::postgres_generic_avro_source(topology &t,
                                                               int32_t partition,
                                                               std::string table,
                                                               const kspp::connect::connection_params& cp,
                                                               std::string id_column,
                                                               std::string ts_column,
                                                               std::shared_ptr<kspp::avro_schema_registry> registry,
                                                               std::chrono::seconds poll_intervall,
                                                               size_t max_items_in_fetch)
      : partition_source<kspp::generic_avro, kspp::generic_avro>(nullptr, partition)
      , _started(false)
      , _exit(false)
      , _impl(partition, table, t.consumer_group(), cp, id_column, ts_column, registry, poll_intervall, max_items_in_fetch)
      , _schema_registry(registry)
      , _schema_id(-1)
      , _commit_chain(table, partition)
      , _parse_errors("parse_errors", "err")
      , _commit_chain_size("commit_chain_size", metric::GAUGE, "msg", [this]() { return _commit_chain.size(); }) {
    this->add_metric(&_commit_chain_size);
    this->add_metrics_tag(KSPP_PROCESSOR_TYPE_TAG, PROCESSOR_NAME);
    this->add_metrics_tag(KSPP_TOPIC_TAG, table);
    this->add_metrics_tag(KSPP_PARTITION_TAG, std::to_string(partition));
  }
}
