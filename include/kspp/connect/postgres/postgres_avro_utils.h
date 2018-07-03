#include <libpq-fe.h>
#include <avro/Generic.hh>
#include <avro/Schema.hh>
#include <kspp/avro/generic_avro.h>
#pragma once

//https://godoc.org/github.com/lib/pq/oid

namespace kspp {
  enum PG_OIDS {
    BOOLOID = 16,
    BYTEAOID = 17,
    CHAROID = 18,
    NAMEOID = 19,
    INT8OID = 20,
    INT2OID = 21,
    PGSQL_INT2VECTOROID = 22,
    INT4OID = 23,
    REGPROCOID = 24,
    TEXTOID = 25,
    OIDOID = 26,
    PGSQL_TIDOID = 27,
    XIDOID = 28,
    CIDOID = 29,
    PGSQL_OIDVECTOROID = 30,
    FLOAT4OID = 700,
    FLOAT8OID = 701,
    DATEOID = 1082,
    TIMEOID = 1083,
    TIMESTAMPOID = 1114,
    TIMESTAMPZOID = 1184,
    NUMERICOID = 1700,
    UUIDOID=2950
  };

  boost::shared_ptr<avro::Schema> schema_for_oid(Oid typid);

  boost::shared_ptr<avro::RecordSchema> schema_for_table_row(std::string schema_name, const PGresult* res);

  boost::shared_ptr<avro::ValidSchema>
  valid_schema_for_table_row(std::string schema_name, const PGresult* res);

  std::string simple_column_name(std::string column_name);

  boost::shared_ptr<avro::RecordSchema>
  schema_for_table_key(std::string schema_name, const std::vector<std::string>& keys, const PGresult* res);

  boost::shared_ptr<avro::ValidSchema>
  valid_schema_for_table_key(std::string schema_name, const std::vector<std::string>& keys, const PGresult* res);

//by index - order in schema and res must match
  std::vector<boost::shared_ptr<avro::GenericDatum>>
  to_avro(boost::shared_ptr<avro::ValidSchema> schema, const PGresult* res);

//by name - the names in schema must match those in res, used for extraction of key's
  std::vector<boost::shared_ptr<avro::GenericDatum>>
  to_avro2(boost::shared_ptr<avro::ValidSchema> schema, const PGresult* res);


  //by name - the names in schema must match those in res, used for extraction of key's
  std::vector<std::shared_ptr<kspp::generic_avro>>
  to_avro3(boost::shared_ptr<avro::ValidSchema> schema, const PGresult* res);


  std::string avro2sql_table_name(boost::shared_ptr<avro::ValidSchema> schema, avro::GenericDatum &datum);
  std::string avro2sql_column_names(boost::shared_ptr<avro::ValidSchema> schema, avro::GenericDatum &datum);

  // SINK UTILS
  std::string avro2sql_create_table_statement(const std::string& tablename, std::string keys, const avro::ValidSchema& schema);
  std::string avro2sql_build_insert_1(const std::string& tablename, const avro::ValidSchema& schema);
  std::string avro2sql_build_upsert_2(const std::string& tablename, const std::string& primary_key, const avro::ValidSchema& schema);
  std::string avro2sql_values(const avro::ValidSchema& schema, const avro::GenericDatum &datum);
  std::string avro2sql_key_values(const avro::ValidSchema& schema, const std::string& key, const avro::GenericDatum &datum);

  std::string avro2sql_delete_key_values(const avro::ValidSchema& schema, const std::string& key, const avro::GenericDatum &datum);
  //std::string avro2sql_build_delete_statement(const std::string& tablename, std::string keys, const avro::ValidSchema& schema);

}

