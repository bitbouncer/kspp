#include <libpq-fe.h>
#include <avro/Generic.hh>
#include <avro/Schema.hh>
#include <kspp/avro/generic_avro.h>

#pragma once

//https://godoc.org/github.com/lib/pq/oid
//https://jdbc.postgresql.org/development/privateapi/constant-values.html

namespace kspp {
  namespace pq {
    // trim from left
    inline std::string &pq_l2trim(std::string &s, const char *t = " \t\n\r\f\v") {
      s.erase(0, s.find_first_not_of(t));
      return s;
    }

// trim from right
    inline std::string &pq_rtrim(std::string &s, const char *t = " \t\n\r\f\v") {
      s.erase(s.find_last_not_of(t) + 1);
      return s;
    }

// trim from left & right
    inline std::string &pq_trim(std::string &s, const char *t = " \t\n\r\f\v") {
      return pq_l2trim(pq_rtrim(s, t), t);
    }

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
      TEXT_ARRAYOID = 1009,
      DATEOID = 1082,
      TIMEOID = 1083,
      TIMESTAMPOID = 1114,
      TIMESTAMPZOID = 1184,
      NUMERICOID = 1700,
      UUIDOID = 2950,
      HSTOREOID = -10000  // fake id used in reverse mapping
    };

    std::shared_ptr<avro::Schema> schema_for_oid(Oid typid);

    std::string simple_column_name(std::string column_name);

    std::shared_ptr<avro::ValidSchema>
    schema_for_table_key(std::string schema_name, const std::vector<std::string> &keys, const PGresult *res);

    std::shared_ptr<avro::ValidSchema> schema_for_table_row(std::string schema_name, const PGresult *res);

    //std::vector<std::shared_ptr<avro::GenericDatum>> to_avro(std::shared_ptr<avro::ValidSchema> schema, const PGresult *res);
    void load_avro_by_name(kspp::generic_avro *avro, PGresult *pgres, size_t row);

    //by index - order in schema and res must match
    //std::vector<std::shared_ptr<avro::GenericDatum>> to_avro(std::shared_ptr<avro::ValidSchema> schema, const PGresult *res);
    //by name - the names in schema must match those in res, used for extraction of key's
    //std::vector<std::shared_ptr<avro::GenericDatum>> to_avro2(std::shared_ptr<avro::ValidSchema> schema, const PGresult *res);
    //by name - the names in schema must match those in res, used for extraction of key's
    //std::vector<std::shared_ptr<kspp::generic_avro>> to_avro3(std::shared_ptr<avro::ValidSchema> schema, const PGresult *res);

    std::string avro2sql_table_name(std::shared_ptr<avro::ValidSchema> schema, avro::GenericDatum &datum);

    std::string avro2sql_column_names(std::shared_ptr<avro::ValidSchema> schema, avro::GenericDatum &datum);

    // SINK UTILS
    std::string
    avro2sql_create_table_statement(const std::string &tablename, std::string keys, const avro::ValidSchema &schema);

    std::string avro2sql_build_insert_1(const std::string &tablename, const avro::ValidSchema &schema);

    std::string avro2sql_build_upsert_2(const std::string &tablename, const std::string &primary_key, const avro::ValidSchema &schema);

    std::string avro2sql_values(const avro::ValidSchema &schema, const avro::GenericDatum &datum);

    std::string avro2sql_key_values(const avro::ValidSchema &schema, const std::string &key, const avro::GenericDatum &datum);

    std::string avro2sql_delete_key_values(const avro::ValidSchema &schema, const std::string &key, const avro::GenericDatum &datum);
  } // namespace pq
} // namespace kspp

