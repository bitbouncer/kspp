#include <kspp/connect/postgres/postgres_avro_utils.h>
#include <algorithm>
#include <string>
#include <boost/make_shared.hpp>
#include <glog/logging.h>
#include <avro/Specific.hh>
#include <avro/Encoder.hh>
#include <avro/Decoder.hh>
#include <avro/Compiler.hh>
#include <avro/Schema.hh>
#include <avro/AvroSerialize.hh>
//#include <iostream>

//inpiration
//http://upp-mirror.googlecode.com/svn/trunk/uppsrc/PostgreSQL/PostgreSQL.cpp

namespace kspp {
  boost::shared_ptr<avro::Schema> schema_for_oid(Oid typid) {
      boost::shared_ptr<avro::Schema> value_schema;
      switch ((PG_OIDS) typid) {
          /* Numeric-like types */
          case BOOLOID:    /* boolean: 'true'/'false' */
              value_schema = boost::make_shared<avro::BoolSchema>();
          break;
          case FLOAT4OID:  /* real, float4: 32-bit floating point number */
              value_schema = boost::make_shared<avro::FloatSchema>();
          break;
          case FLOAT8OID:  /* double precision, float8: 64-bit floating point number */
              value_schema = boost::make_shared<avro::DoubleSchema>();
          break;
          case INT2OID:    /* smallint, int2: 16-bit signed integer */
          case INT4OID:    /* integer, int, int4: 32-bit signed integer */
              value_schema = boost::make_shared<avro::IntSchema>();
          break;
          case INT8OID:    /* bigint, int8: 64-bit signed integer */
              //case CASHOID:    /* money: monetary amounts, $d,ddd.cc, stored as 64-bit signed integer */
          case OIDOID:     /* oid: Oid is unsigned int */
              //case REGPROCOID: /* regproc: RegProcedure is Oid */
          case XIDOID:     /* xid: TransactionId is uint32 */
          case CIDOID:     /* cid: CommandId is uint32 */
              value_schema = boost::make_shared<avro::LongSchema>();
          break;
          case NUMERICOID: /* numeric(p, s), decimal(p, s): arbitrary precision number */
              value_schema = boost::make_shared<avro::DoubleSchema>();
          break;

          /* Date/time types. We don't bother with abstime, reltime and tinterval (which are based
          * on Unix timestamps with 1-second resolution), as they are deprecated. */

          //case DATEOID:        /* date: 32-bit signed integer, resolution of 1 day */
          //    //return schema_for_date(); not implemented YET
          //    value_schema = boost::make_shared<avro::Node>(avro::AVRO_STRING);
          //    break;
          //    // this is wrong...

          case TIMEOID:        /* time without time zone: microseconds since start of day */
              value_schema = boost::make_shared<avro::LongSchema>();
          break;
          //case TIMETZOID:      /* time with time zone, timetz: time of day with time zone */
          //    //value_schema = schema_for_time_tz(); NOT IMPEMENTED YET
          //    value_schema = boost::make_shared<avro::Node>(avro::AVRO_STRING);
          //    break;
          case TIMESTAMPOID:   /* timestamp without time zone: datetime, microseconds since epoch */
            value_schema = boost::make_shared<avro::LongSchema>();
          break;

          //    // return schema_for_timestamp(false);NOT IMPEMENTED YET
          //    value_schema = boost::make_shared<avro::Node>(avro::AVRO_STRING);
          //    break;
          //case TIMESTAMPTZOID: /* timestamp with time zone, timestamptz: datetime with time zone */
          //    //return schema_for_timestamp(true); NOT IMPEMENTED YET
          //    value_schema = boost::make_shared<avro::Node>(avro::AVRO_STRING);
          //    break;

          //case INTERVALOID:    /* @ <number> <units>, time interval */
          //    //value_schema = schema_for_interval(); NOT IMPEMENTED YET
          //    value_schema = boost::make_shared<avro::Node>(avro::AVRO_STRING);
          //    break;

          /* Binary string types */
          case BYTEAOID:   /* bytea: variable-length byte array */
              value_schema = boost::make_shared<avro::BytesSchema>();
          break;
          //case BITOID:     /* fixed-length bit string */
          //case VARBITOID:  /* variable-length bit string */
          case UUIDOID:    /* UUID datatype */
            value_schema = boost::make_shared<avro::StringSchema>();
          break;
          //case LSNOID:     /* PostgreSQL LSN datatype */
          //case MACADDROID: /* XX:XX:XX:XX:XX:XX, MAC address */
          //case INETOID:    /* IP address/netmask, host address, netmask optional */
          //case CIDROID:    /* network IP address/netmask, network address */

          /* Geometric types */
          //case POINTOID:   /* geometric point '(x, y)' */
          //case LSEGOID:    /* geometric line segment '(pt1,pt2)' */
          //case PATHOID:    /* geometric path '(pt1,...)' */
          //case BOXOID:     /* geometric box '(lower left,upper right)' */
          //case POLYGONOID: /* geometric polygon '(pt1,...)' */
          //case LINEOID:    /* geometric line */
          //case CIRCLEOID:  /* geometric circle '(center,radius)' */

          /* range types... decompose like array types? */

          /* JSON types */
          //case JSONOID:    /* json: Text-based JSON */
          //case JSONBOID:   /* jsonb: Binary JSON */

          /* String-like types: fall through to the default, which is to create a string representation */
          case CHAROID:    /* "char": single character */
          case NAMEOID:    /* name: 63-byte type for storing system identifiers */
          case TEXTOID:    /* text: variable-length string, no limit specified */
              //case BPCHAROID:  /* character(n), char(length): blank-padded string, fixed storage length */
              //case VARCHAROID: /* varchar(length): non-blank-padded string, variable storage length */
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
      //avro_schema_decref(null_schema);
      //avro_schema_decref(value_schema);
      return union_schema;
  }


  boost::shared_ptr<avro::RecordSchema> schema_for_table_row(std::string schema_name, const PGresult* res) {
      boost::shared_ptr<avro::RecordSchema> record_schema = boost::make_shared<avro::RecordSchema>(schema_name);

      int nFields = PQnfields(res);
      for (int i = 0; i < nFields; i++) {
          Oid col_type = PQftype(res, i);
          std::string col_name = PQfname(res, i);
          boost::shared_ptr<avro::Schema> col_schema = schema_for_oid(col_type);
          /* TODO ensure that names abide by Avro's requirements */
          record_schema->addField(col_name, *col_schema);
      }
      return record_schema;
  }

  boost::shared_ptr<avro::RecordSchema>
  schema_for_table_key(std::string schema_name, const std::vector<std::string> &keys, const PGresult* res) {
      boost::shared_ptr<avro::RecordSchema> record_schema = boost::make_shared<avro::RecordSchema>(schema_name);
      for (std::vector<std::string>::const_iterator i = keys.begin(); i != keys.end(); i++) {
          int column_index = PQfnumber(res, i->c_str());
          assert(column_index >= 0);
          if (column_index >= 0) {
              Oid col_type = PQftype(res, column_index);
              boost::shared_ptr<avro::Schema> col_schema = schema_for_oid(col_type);
              /* TODO ensure that names abide by Avro's requirements */
              record_schema->addField(*i, *col_schema);
          }
      }
      return record_schema;
  }

  boost::shared_ptr<avro::ValidSchema>
  valid_schema_for_table_row(std::string schema_name, const PGresult* res) {
      return boost::make_shared<avro::ValidSchema>(*schema_for_table_row(schema_name, res));
  }

  boost::shared_ptr<avro::ValidSchema>
  valid_schema_for_table_key(std::string schema_name, const std::vector<std::string> &keys, const PGresult* res) {
    if (keys.size()==0)
      return boost::make_shared<avro::ValidSchema>(avro::NullSchema());
    else
      return boost::make_shared<avro::ValidSchema>(*schema_for_table_key(schema_name, keys, res));
  }

//// this is done by assuming all fields are in the same order... ???
//  std::vector<boost::shared_ptr<avro::GenericDatum>>
//  to_avro(boost::shared_ptr<avro::ValidSchema> schema, const PGresult* res) {
//      size_t nFields = PQnfields(res);
//      int nRows = PQntuples(res);
//      std::vector<boost::shared_ptr<avro::GenericDatum>> result;
//      result.reserve(nRows);
//      for (int i = 0; i < nRows; i++) {
//          boost::shared_ptr<avro::GenericDatum> gd = boost::make_shared<avro::GenericDatum>(*schema);
//          assert(gd->type() == avro::AVRO_RECORD);
//          avro::GenericRecord &record(gd->value<avro::GenericRecord>());
//          for (int j = 0; j < nFields; j++) {
//              if (record.fieldAt(j).type() != avro::AVRO_UNION) {
//                  LOG(FATAL) << "unexpected schema - bailing out";
//                  break;
//              }
//
//              avro::GenericUnion &au(record.fieldAt(j).value<avro::GenericUnion>());
//
//              if (PQgetisnull(res, i, j) == 1) {
//                  au.selectBranch(0); // NULL branch - we hope..
//                  assert(au.datum().type() == avro::AVRO_NULL);
//              } else {
//                  au.selectBranch(1);
//                  avro::GenericDatum &avro_item(au.datum());
//                  const char *val = PQgetvalue(res, i, j);
//
//                  switch (avro_item.type()) {
//                      case avro::AVRO_STRING:
//                          avro_item.value<std::string>() = val;
//                      break;
//                      case avro::AVRO_BYTES:
//                          avro_item.value<std::string>() = val;
//                      break;
//                      case avro::AVRO_INT:
//                          avro_item.value<int32_t>() = atoi(val);
//                      break;
//                      case avro::AVRO_LONG:
//                          avro_item.value<int64_t>() = std::stoull(val);
//                      break;
//                      case avro::AVRO_FLOAT:
//                          avro_item.value<float>() = (float) atof(val);
//                      break;
//                      case avro::AVRO_DOUBLE:
//                          avro_item.value<double>() = atof(val);
//                      break;
//                      case avro::AVRO_BOOL:
//                        avro_item.value<bool>() = (val[0]=='t' || val[0]=='T' || val[0]=='1');
//                      break;
//                      case avro::AVRO_RECORD:
//                      case avro::AVRO_ENUM:
//                      case avro::AVRO_ARRAY:
//                      case avro::AVRO_MAP:
//                      case avro::AVRO_UNION:
//                      case avro::AVRO_FIXED:
//                      case avro::AVRO_NULL:
//                      default:
//                          LOG(FATAL)  << "unexpected / non supported type e:" << avro_item.type();
//                  }
//              }
//          }
//          result.push_back(gd);
//
//          //std::cerr << std::endl;
//
//          //avro::encode(*encoder, *gd);
//          // I create a DataFileWriter and i write my pair of ValidSchema and GenericValue
//          //avro::DataFileWriter<Pair> dataFileWriter("test.bin", schema);
//          //dataFileWriter.write(p);
//          //dataFileWriter.close();
//      }
//      return result;
//  }
//
//// this is done by mapping schema field names to result columns...
//  std::vector<boost::shared_ptr<avro::GenericDatum>>
//  to_avro2(boost::shared_ptr<avro::ValidSchema> schema, const PGresult* res) {
//      int nRows = PQntuples(res);
//      std::vector<boost::shared_ptr<avro::GenericDatum>> result;
//      result.reserve(nRows);
//      for (int i = 0; i < nRows; i++) {
//          boost::shared_ptr<avro::GenericDatum> gd = boost::make_shared<avro::GenericDatum>(*schema);
//          assert(gd->type() == avro::AVRO_RECORD);
//          avro::GenericRecord &record(gd->value<avro::GenericRecord>());
//          size_t nFields = record.fieldCount();
//          for (int j = 0; j < nFields; j++) {
//              if (record.fieldAt(j).type() != avro::AVRO_UNION) {
//                  LOG(FATAL) << "unexpected schema - bailing out, type:" << record.fieldAt(j).type();
//                  break;
//              }
//              avro::GenericUnion &au(record.fieldAt(j).value<avro::GenericUnion>());
//
//              const std::string &column_name = record.schema()->nameAt(j);
//
//              //which pg column has this value?
//              int column_index = PQfnumber(res, column_name.c_str());
//              if (column_index < 0) {
//                  LOG(FATAL) << "unknown column - bailing out: " << column_name;
//                  break;
//              }
//
//              if (PQgetisnull(res, i, column_index) == 1) {
//                  au.selectBranch(0); // NULL branch - we hope..
//                  assert(au.datum().type() == avro::AVRO_NULL);
//              } else {
//                  au.selectBranch(1);
//                  avro::GenericDatum &avro_item(au.datum());
//                  const char *val = PQgetvalue(res, i, j);
//
//                  switch (avro_item.type()) {
//                      case avro::AVRO_STRING:
//                          avro_item.value<std::string>() = val;
//                      break;
//                      case avro::AVRO_BYTES:
//                          avro_item.value<std::string>() = val;
//                      break;
//                      case avro::AVRO_INT:
//                          avro_item.value<int32_t>() = atoi(val);
//                      break;
//                      case avro::AVRO_LONG:
//                          avro_item.value<int64_t>() = std::stoull(val);
//                      break;
//                      case avro::AVRO_FLOAT:
//                          avro_item.value<float>() = (float) atof(val);
//                      break;
//                      case avro::AVRO_DOUBLE:
//                          avro_item.value<double>() = atof(val);
//                      break;
//                      case avro::AVRO_BOOL:
//                        avro_item.value<bool>() = (val[0]=='t' || val[0]=='T' || val[0]=='1');
//                      break;
//                      case avro::AVRO_RECORD:
//                      case avro::AVRO_ENUM:
//                      case avro::AVRO_ARRAY:
//                      case avro::AVRO_MAP:
//                      case avro::AVRO_UNION:
//                      case avro::AVRO_FIXED:
//                      case avro::AVRO_NULL:
//                      default:
//                         LOG(FATAL) << "unexpected / non supported type e:" << avro_item.type();
//                  }
//              }
//          }
//          result.push_back(gd);
//
//          //std::cerr << std::endl;
//
//          //avro::encode(*encoder, *gd);
//          // I create a DataFileWriter and i write my pair of ValidSchema and GenericValue
//          //avro::DataFileWriter<Pair> dataFileWriter("test.bin", schema);
//          //dataFileWriter.write(p);
//          //dataFileWriter.close();
//      }
//      return result;
//  }


/*
std::string postgres_type(avro::Type t)
{

}
*/


//std::string build_fields()
//{
//    auto schema = confound_orders_t::valid_schema();
//    avro::GenericDatum gd(schema);
//    auto t = gd.type();
//    assert(t == avro::AVRO_RECORD);
//    avro::GenericRecord gr = gd.value<avro::GenericRecord>();
//    for (int i = 0; i != gr.fieldCount(); i++)
//    {
//        std::cerr << i << " -> ";
//        std::cerr << gr.schema()->nameAt(i);
//        auto leaf = gr.schema()->leafAt(i);
//        switch (leaf->type())
//        {
//        case avro::AVRO_ARRAY:
//        case avro::AVRO_BOOL:
//        case avro::AVRO_BYTES:
//            std::cerr << ", type:" << leaf->type() << std::endl;
//
//        case avro::AVRO_UNION:
//
//            if (leaf->type() == avro::AVRO_UNION)
//            {
//                //this should be a NULLable field and the real type.
//                for (int j = 0; j != leaf->)
//
//            }
//        };
//        /*
//        auto f = gr.fieldAt(i);
//        if (f.isUnion())
//        {
//        std::cerr << f.isUnion() ? "union";
//        f.
//        }
//        */
//    }
//    return "*";
//}

/*
std::string escapeSQL(SQLConnection & connection, const std::string & dataIn)
{
    // This might be better as an assertion or exception, if an empty string
    // is considered an error. Depends on your requirements for this function.
    if (dataIn.empty())
    {
        return "";
    }

    const std::size_t dataInLen = dataIn.length();
    std::vector<char> temp((dataInLen * 2) + 1, '\0');
    mysql_real_escape_string(&connection, temp.data(), dataIn.c_str(), dataInLen);

    return temp.data();
    // Will create a new string but the compiler is likely 
    // to optimize this to a cheap move operation (C++11).
}
*/

  class IsChars {
  public:
    IsChars(const char *charsToRemove) : chars(charsToRemove) {};

    bool operator()(char c) {
        for (const char *testChar = chars; *testChar != 0; ++testChar) {
            if (*testChar == c) { return true; }
        }
        return false;
    }

  private:
    const char *chars;
  };


  std::string avro2sql_table_name(boost::shared_ptr<avro::ValidSchema> schema, avro::GenericDatum &datum) {
      auto r = schema->root();
      assert(r->type() == avro::AVRO_RECORD);
      if (r->hasName()) {
          std::string name = r->name();

          //since we use convention tablename.key / table_name.value (until we know what bottledwater does...)
          // return namesapace as table name
          std::size_t found = name.find_first_of(".");
          if (found != std::string::npos) {
              std::string ns = name.substr(0, found);
              return ns;
          }
          return r->name();
      }
      assert(false);
      return "unknown_table_name";
  }

  std::string avro2sql_column_names(boost::shared_ptr<avro::ValidSchema> schema, avro::GenericDatum &datum) {
      auto r = schema->root();
      assert(r->type() == avro::AVRO_RECORD);
      std::string s = "(";
      size_t sz = r->names();
      for (int i = 0; i != sz; ++i) {
          s += r->nameAt(i);
          if (i != sz - 1)
              s += ",";
      }
      s += ")";
      /*
      schema->toJson(std::cerr);
      std::cerr << std::endl;
      return "unknown";
      */
      return s;
  }



  static Oid avro_type_to_oid(avro::Type avro_type)
  {
      switch (avro_type)
      {
          case avro::AVRO_STRING: return TEXTOID;
          case avro::AVRO_BYTES:  return BYTEAOID;
          case avro::AVRO_INT:    return INT4OID;
          case avro::AVRO_LONG:   return INT8OID;
          case avro::AVRO_FLOAT:  return FLOAT4OID;
          case avro::AVRO_DOUBLE: return FLOAT8OID;
          case avro::AVRO_BOOL:   return BOOLOID;
          case avro::AVRO_UNION:
          case avro::AVRO_RECORD:
          case avro::AVRO_ENUM:
          case avro::AVRO_ARRAY:
          case avro::AVRO_MAP:

          case avro::AVRO_FIXED:
          case avro::AVRO_NULL:
          default:
              LOG(FATAL) << "unsupported / non supported type e:" << avro_type;
      }
      return TEXTOID;
  }

  static std::string to_string(Oid oid)
  {
      switch ((PG_OIDS)oid)
      {
          case BOOLOID:    return "boolean";
          case FLOAT4OID:  return "float4";
          case FLOAT8OID:  return "float8";
          case INT2OID:    return "smallint";
          case INT4OID:    return "integer";
          case INT8OID:    return "bigint";
          case BYTEAOID:   return "bytea";
          case CHAROID:    return "char";
          case NAMEOID:    return "name";
          case TEXTOID:    return "text";
          default:
              LOG(FATAL) << "unsupported / non supported type e:" << oid;
          break;
      }
      return "unsupported";
  }

  // this is hackish - if the column is a union type we assume that it can be null
  static bool avro2sql_is_column_nullable(const avro::GenericDatum &column) {
    auto t = column.type();
    return (t==avro::AVRO_UNION);
  }

  std::string avro2sql_create_table_statement(const std::string& tablename, std::string keys, const avro::ValidSchema& schema)
  {
      auto root = schema.root();
      assert(root->type() == avro::AVRO_RECORD);
      std::string s = "CREATE TABLE " + tablename + " (\n";
      size_t sz = root->names();
      for (int i = 0; i != sz; ++i)
      {
        auto leaf = root->leafAt(i);

        // nullable or not??
        if (leaf->type() == avro::AVRO_UNION){
          //auto avro_null = leaf->leafAt(0);
          auto avro_val = leaf->leafAt(1);
          s += root->nameAt(i) + " " + to_string(avro_type_to_oid(avro_val->type()));
        } else {
          s += root->nameAt(i) + " " + to_string(avro_type_to_oid(root->leafAt(i)->type())) + " NOT NULL";
        }

        if (i != sz - 1)
              s += ",";
      }

      if (keys.size()>0)
          s += ", PRIMARY KEY(" + keys + ") ";

      s += ")";
      return s;
  }


  std::string avro2sql_build_insert_1(const std::string& tablename, const avro::ValidSchema& schema){
      auto r = schema.root();
      assert(r->type() == avro::AVRO_RECORD);
      std::string s = "INSERT INTO " + tablename + "(\n";
      size_t sz = r->names();
      for (int i = 0; i != sz; ++i)
      {
          s += r->nameAt(i);
          if (i != sz - 1)
              s += ",";
      }
      s += ") VALUES\n";
      return s;
  }

  std::string avro2sql_build_upsert_2(const std::string& tablename, const std::string& primary_key, const avro::ValidSchema& schema){
    auto r = schema.root();
    assert(r->type() == avro::AVRO_RECORD);
    std::string s = "ON CONFLICT (" + primary_key + ") DO UPDATE SET (\n";
    size_t sz = r->names();
    for (int i = 0; i != sz; ++i)
    {
      s += r->nameAt(i);
      if (i != sz - 1)
        s += ",";
    }
    s += ") = \n(";

    for (int i = 0; i != sz; ++i)
    {
      s += "EXCLUDED." + r->nameAt(i);
      if (i != sz - 1)
        s += ",";
    }
    s += ")\n";

    return s;
  }


  std::string escapeSQLstring(std::string src) {
      //we should escape the sql string instead of doing this... - for now this removes ' characters in string
      src.erase(std::remove_if(src.begin(), src.end(), IsChars("'")), src.end());
      return std::string("'" + src + "'"); // we have to do real escaping here to prevent injection attacks  TBD
  }

  // only maps simple types to value
  static std::string avro2sql_simple_column_value(const avro::GenericDatum &column) {
    auto t = column.type();
    switch (t) {
      // nullable columns are represented as union of NULL and value
      // parse those recursive
      case avro::AVRO_UNION: {
        const avro::GenericUnion& au(column.value<avro::GenericUnion>());
        return avro2sql_simple_column_value(au.datum());
      }
      case avro::AVRO_NULL:
        return "NULL";
        break;
      case avro::AVRO_STRING:
        return escapeSQLstring(column.value<std::string>());
        break;
      case avro::AVRO_BYTES:
        return column.value<std::string>();
        break;
      case avro::AVRO_INT:
        return std::to_string(column.value<int32_t>());
        break;
      case avro::AVRO_LONG:
        return std::to_string(column.value<int64_t>());
        break;
      case avro::AVRO_FLOAT:
        return std::to_string(column.value<float>());
        break;
      case avro::AVRO_DOUBLE:
        return std::to_string(column.value<double>());
        break;
      case avro::AVRO_BOOL:
        return column.value<bool>() ? "True" : "False";
        break;
      case avro::AVRO_RECORD:
      case avro::AVRO_ENUM:
      case avro::AVRO_ARRAY:
      case avro::AVRO_MAP:

      case avro::AVRO_FIXED:
      default:
        LOG(FATAL) << "unexpected / non supported type e:" << column.type();
    }

  }


  // handles both nullable and non nullable columns
  std::string avro2sql_values(const avro::ValidSchema& schema, const avro::GenericDatum &datum) {
    std::string result = "(";
    assert(datum.type() == avro::AVRO_RECORD);
    const avro::GenericRecord& record(datum.value<avro::GenericRecord>());
    size_t nFields = record.fieldCount();
    for (int i = 0; i < nFields; i++) {
      std::string val = avro2sql_simple_column_value(record.fieldAt(i));
      if (i < (nFields - 1))
        result += val + ", ";
      else
        result += val + ")";
    }
    return result;
  }

  // TODO mutiple keys
  std::string avro2sql_key_values(const avro::ValidSchema& schema, const std::string& key, const avro::GenericDatum &datum){
    assert(datum.type() == avro::AVRO_RECORD);
    const avro::GenericRecord& record(datum.value<avro::GenericRecord>());
    std::string result;
    auto x = record.field(key);
    result += avro2sql_simple_column_value(x);
    return result;
  }

  std::string avro2sql_delete_key_values(const avro::ValidSchema& schema, const std::string& key, const avro::GenericDatum &datum){
    if (datum.type() == avro::AVRO_RECORD) {
      auto root = schema.root();
      assert(root->type() == avro::AVRO_RECORD);
      std::string result = "(";
      const avro::GenericRecord &record(datum.value<avro::GenericRecord>());
      size_t nFields = record.fieldCount();
      for (int i = 0; i < nFields; i++) {
        result += root->nameAt(i) + "=";
        std::string val = avro2sql_simple_column_value(record.fieldAt(i));
        if (i < (nFields - 1))
          result += "=" + val + " AND ";
        else
          result += "=" + val + ")";
      }
      return result;
    } else {
      return key + "=" + avro2sql_simple_column_value(datum);
    }
  }

} // namespace