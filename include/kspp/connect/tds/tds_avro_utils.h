#include <avro/Generic.hh>
#include <avro/Schema.hh>
#include <kspp/avro/avro_generic.h>
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
    boost::shared_ptr<avro::RecordSchema> schema_for_table_row(std::string schema_name, DBPROCESS *context);
    //boost::shared_ptr<avro::Schema> schema_for_table_ts(std::string ts_field, DBPROCESS *context);
  }
}