#include <kspp/connect/elasticsearch/elasticsearch_generic_avro_sink.h>

namespace kspp {
  elasticsearch_generic_avro_sink::elasticsearch_generic_avro_sink(topology &t,
                                                         std::string table,
                                                         std::string base_url,
                                                         std::string user,
                                                         std::string password,
                                                         std::string id_column,
                                                         std::shared_ptr<kspp::avro_schema_registry> schema_registry)
      : _impl(table, base_url, user, password, id_column)
      , _schema_registry(schema_registry){
  }
}
