#include <kspp/connect/tds/tds_avro_utils.h>
#include <boost/make_shared.hpp>
#include <glog/logging.h>
#include <avro/Specific.hh>
namespace kspp{
  namespace tds {

    boost::shared_ptr<avro::Schema> schema_for_oid(TDS_OIDS id) {
      boost::shared_ptr<avro::Schema> value_schema;
      switch (id) {
        case SYBBIT:    /* bit 0/1 */
          value_schema = boost::make_shared<avro::BoolSchema>();
          break;

          /* Numeric-like types */
          //case FLOAT4OID:  /* real, float4: 32-bit floating point number */
          //  value_schema = boost::make_shared<avro::FloatSchema>();
          //  break;
        case SYBFLT8:  /* double precision, float8: 64-bit floating point number */
          value_schema = boost::make_shared<avro::DoubleSchema>();
          break;
        case SYBINT2:    /* smallint, int2: 16-bit signed integer */
        case SYBINT4:    /* integer, int, int4: 32-bit signed integer */
          value_schema = boost::make_shared<avro::IntSchema>();
          break;
        case SYBINT8:    /* bigint, int8: 64-bit signed integer */
          value_schema = boost::make_shared<avro::LongSchema>();
          break;

          // this is wrong
        case SYBMSDATETIME2:
          value_schema = boost::make_shared<avro::StringSchema>();
          break;

          /* String-like types: fall through to the default, which is to create a string representation */
        case SYBCHAR:
        default:
          value_schema = boost::make_shared<avro::StringSchema>();
          break;
      }
      /* Make a union of value_schema with null. Some types are already a union,
    * in which case they must include null as the first branch of the union,
    * and return directly from the function without getting here (otherwise
    * we'd get a union inside a union, which is not valid Avro). */
      boost::shared_ptr<avro::Schema> null_schema = boost::make_shared<avro::NullSchema>();
      boost::shared_ptr<avro::UnionSchema> union_schema = boost::make_shared<avro::UnionSchema>();
      union_schema->addType(*null_schema);
      union_schema->addType(*value_schema);
      return union_schema;
    }

    boost::shared_ptr<avro::RecordSchema> schema_for_table_row(std::string schema_name, DBPROCESS *context) {
      boost::shared_ptr<avro::RecordSchema> record_schema = boost::make_shared<avro::RecordSchema>(schema_name);

      int ncols = dbnumcols(context);
      for (int i = 0; i < ncols; i++) {
        const char *col_name = dbcolname(context, i + 1);
        int col_type = dbcoltype(context, i + 1);
        boost::shared_ptr<avro::Schema> col_schema = schema_for_oid((TDS_OIDS) col_type);
        /* TODO ensure that names abide by Avro's requirements */
        record_schema->addField(col_name, *col_schema);
      }
      return record_schema;
    }



  }
}