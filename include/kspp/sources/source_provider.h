#include <kspp/kspp.h>
#include <kspp/cluster_config.h>
#include <kspp/utils/offset_storage_provider.h>
#include <kspp/sources/kafka_source.h>

#ifdef KSPP_GRPC
#include <kspp/connect/bitbouncer/grpc_avro_source.h>
#endif

#ifdef KSPP_POSTGRES
#include <kspp/connect/postgres/postgres_consumer.h>
#endif

#pragma once

namespace kspp{
  struct source_parts {
    enum protocol_type_t {
      NONE, KAFKA, AVRO, TDS, POSTGRES, BB_GRPC
    };

    protocol_type_t protocol;
    std::string host;
    int port;
    std::string topic;
    int partition;
  };

  source_parts split_url_parts(std::string uri);

  template<class K, class V>
    std::shared_ptr<partition_source<K, V> > make_source(std::shared_ptr<cluster_config> config, std::shared_ptr<offset_storage> offset_store, std::string uri){
    auto parts = split_url_parts(uri);

      switch (parts.protocol){
        case source_parts::NONE:
          return nullptr;
        case source_parts::KAFKA: {
          // kafka://topic:partition
          std::make_shared<kafka_source<K, V, avro_serdes, avro_serdes>>(config, parts.partition, parts.topic, config->avro_serdes(), config->avro_serdes());
        }
        case source_parts::AVRO:
          return nullptr;
        case source_parts::TDS:
          return nullptr;
#ifdef KSPP_POSTGRES
        case source_parts::POSTGRES: {
          std::string consumer_group = "";
          kspp::connect::connection_params connection_params;
          kspp::connect::table_params table_params;
          std::string query;
          std::string id_column;
          std::string ts_column;
          //std::make_shared<postgres_consumer<K, V>>(parts.partition, parts.topic, consumer_group, connection_params, table_params, query, id_column, ts_column, config->get_schema_registry());
        }
          return nullptr;
#endif

#ifdef KSPP_GRPC
        case source_parts::BB_GRPC: {
          // bb://host:port/topic:partition
          std::string uri;
          std::string api_key;
          std::string secret_access_key;
          return std::make_shared<grpc_avro_source<K, V>>(config, parts.partition, parts.topic, offset_store, uri, api_key, secret_access_key);
        }
#endif
      }
    }
}
