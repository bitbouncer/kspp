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

    std::string simple_column_name(std::string column_name) {
      std::string simple=column_name;
      size_t found = simple.find_last_of('.');
      if (found!=std::string::npos)
        simple = simple.substr(found+1);
      return simple;
    }

    int find_column_by_name(DBPROCESS *stream, const std::string& name){
      auto ncols = dbnumcols(stream);
      for(int i=0; i!=ncols; ++i){
        if (name == dbcolname(stream, i+1))
          return i;
      }
      return -1;
    }

    std::shared_ptr<avro::ValidSchema> schema_for_table_row(std::string schema_name, DBPROCESS *context) {
      avro::RecordSchema record_schema(schema_name);
      int ncols = dbnumcols(context);
      for (int i = 0; i < ncols; i++) {
        const char *col_name = dbcolname(context, i + 1);
        int col_type = dbcoltype(context, i + 1);
        boost::shared_ptr<avro::Schema> col_schema = schema_for_oid((TDS_OIDS) col_type);
        /* TODO ensure that names abide by Avro's requirements */
        record_schema.addField(col_name, *col_schema);
      }
      auto result = std::make_shared<avro::ValidSchema>(record_schema);
      return result;
    }

    // if we have a freeform select statement we might need to specify id and ts columns as a.id and b.ts if the fields occur in several tables
    // strip this away
    std::shared_ptr<avro::ValidSchema> schema_for_table_key(std::string schema_name, const std::vector<std::string>& keys, DBPROCESS *context) {
      avro::RecordSchema record_schema(schema_name);
      int ncols = dbnumcols(context);
      for (std::vector<std::string>::const_iterator key = keys.begin(); key != keys.end(); key++) {
        std::string simple_key = simple_column_name(*key);
        for (int i = 0; i < ncols; i++) {
          const char *col_name = dbcolname(context, i + 1);
          if (simple_key == col_name) {
            int col_type = dbcoltype(context, i + 1);
            boost::shared_ptr<avro::Schema> col_schema = schema_for_oid((TDS_OIDS) col_type);
            /* TODO ensure that names abide by Avro's requirements */
            record_schema.addField(col_name, *col_schema);
            break;
          }
        }
      }
      auto result = std::make_shared<avro::ValidSchema>(record_schema);
      return result;
    }
  }
}