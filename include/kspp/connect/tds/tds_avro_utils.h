#include <avro/Generic.hh>
#include <avro/Schema.hh>
//#include <kspp/avro/generic_avro.h>
#include <sybdb.h>
#pragma once
namespace kspp {
  namespace tds {
    enum TDS_OIDS {
      SYBCHAR = 47,
      SYBBIT = 50,
      SYBINT2 = 52,
      SYBINT4 = 56,
      SYBFLT8 = 62,

 // MS only types
      SYBINT8 = 127,
      SYBMSUDT = 240,
      SYBMSDATETIME2 = 42
    };

    boost::shared_ptr<avro::Schema> schema_for_oid(TDS_OIDS typid);
    std::shared_ptr<avro::ValidSchema> schema_for_table_row(std::string schema_name, DBPROCESS *context);

    std::shared_ptr<avro::ValidSchema> schema_for_table_key(std::string schema_name, const std::vector<std::string>& keys, DBPROCESS *context);
    std::string simple_column_name(std::string column_name);
    int find_column_by_name(DBPROCESS *stream, const std::string& name);
  }
}