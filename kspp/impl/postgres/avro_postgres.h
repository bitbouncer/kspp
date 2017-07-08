#include <avro/Generic.hh>
#include <avro/Schema.hh>

#ifdef WIN32
#include <libpq-fe.h>
#else

#include <postgresql/libpq-fe.h>

#endif
//#include <postgres_asio/postgres_asio.h>

#pragma once

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
  NUMERICOID = 1700
};

std::shared_ptr<avro::Schema> schema_for_oid(Oid typid);

std::shared_ptr<avro::RecordSchema> schema_for_table_row(std::string schema_name, std::shared_ptr<PGresult> res);

std::shared_ptr<avro::ValidSchema> valid_schema_for_table_row(std::string schema_name, std::shared_ptr<PGresult> res);

std::shared_ptr<avro::RecordSchema>
schema_for_table_key(std::string schema_name, const std::vector<std::string> &keys, std::shared_ptr<PGresult> res);

std::shared_ptr<avro::ValidSchema>
valid_schema_for_table_key(std::string schema_name, const std::vector<std::string> &keys,
                           std::shared_ptr<PGresult> res);

//by index - order in schema and res must match
std::vector<std::shared_ptr<avro::GenericDatum>>
to_avro(std::shared_ptr<avro::ValidSchema> schema, std::shared_ptr<PGresult> res);

//by name - the names in schema must match those in res, used for extraction of key's
std::vector<std::shared_ptr<avro::GenericDatum>>
to_avro2(std::shared_ptr<avro::ValidSchema> schema, std::shared_ptr<PGresult> res);


std::string avro2sql_values(std::shared_ptr<avro::ValidSchema> schema, avro::GenericDatum &datum);

std::string avro2sql_table_name(std::shared_ptr<avro::ValidSchema> schema, avro::GenericDatum &datum);

std::string avro2sql_column_names(std::shared_ptr<avro::ValidSchema> schema, avro::GenericDatum &datum);



//avro::NodePtr schema_for_table_row(std::shared_ptr<PGresult> res);