#include <kspp/connect/postgres/postgres_generic_avro_sink.h>
namespace kspp {
  postgres_generic_avro_sink::postgres_generic_avro_sink(topology &t,
                               std::string table,
                               std::string connect_string,
                               std::string id_column,
                               std::shared_ptr<kspp::avro_schema_registry> schema_registry)
  : _impl(table, connect_string, id_column)
  , _schema_registry(schema_registry){
  }
}
