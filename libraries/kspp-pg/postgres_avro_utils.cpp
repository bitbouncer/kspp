#include <kspp-pg/postgres_avro_utils.h>
#include <algorithm>
#include <string>
#include <glog/logging.h>
#include <avro/Specific.hh>
#include <avro/Encoder.hh>
#include <avro/Decoder.hh>
#include <avro/Compiler.hh>
#include <avro/Schema.hh>
#include <avro/AvroSerialize.hh>
#include <boost/algorithm/string.hpp>
#include <kspp/utils/string_utils.h>
//inpiration
//http://upp-mirror.googlecode.com/svn/trunk/uppsrc/PostgreSQL/PostgreSQL.cpp

namespace kspp {
  namespace pq {
    std::shared_ptr<avro::Schema> schema_for_oid(Oid typid) {
      std::shared_ptr<avro::Schema> value_schema;
      switch ((PG_OIDS) typid) {
        /* Numeric-like types */
        case BOOLOID:    /* boolean: 'true'/'false' */
          value_schema = std::make_shared<avro::BoolSchema>();
          break;
        case FLOAT4OID:  /* real, float4: 32-bit floating point number */
          value_schema = std::make_shared<avro::FloatSchema>();
          break;
        case FLOAT8OID:  /* double precision, float8: 64-bit floating point number */
          value_schema = std::make_shared<avro::DoubleSchema>();
          break;
        case INT2OID:    /* smallint, int2: 16-bit signed integer */
        case INT4OID:    /* integer, int, int4: 32-bit signed integer */
          value_schema = std::make_shared<avro::IntSchema>();
          break;
        case INT8OID:    /* bigint, int8: 64-bit signed integer */
          //case CASHOID:    /* money: monetary amounts, $d,ddd.cc, stored as 64-bit signed integer */
        case OIDOID:     /* oid: Oid is unsigned int */
          //case REGPROCOID: /* regproc: RegProcedure is Oid */
        case XIDOID:     /* xid: TransactionId is uint32 */
        case CIDOID:     /* cid: CommandId is uint32 */
          value_schema = std::make_shared<avro::LongSchema>();
          break;
        case NUMERICOID: /* numeric(p, s), decimal(p, s): arbitrary precision number */
          value_schema = std::make_shared<avro::DoubleSchema>();
          break;

          /* Date/time types. We don't bother with abstime, reltime and tinterval (which are based
          * on Unix timestamps with 1-second resolution), as they are deprecated. */

          //case DATEOID:        /* date: 32-bit signed integer, resolution of 1 day */
          //    //return schema_for_date(); not implemented YET
          //    value_schema = boost::make_shared<avro::Node>(avro::AVRO_STRING);
          //    break;
          //    // this is wrong...

        case TIMEOID:        /* time without time zone: microseconds since start of day */
          value_schema = std::make_shared<avro::LongSchema>();
          break;
          //case TIMETZOID:      /* time with time zone, timetz: time of day with time zone */
          //    //value_schema = schema_for_time_tz(); NOT IMPEMENTED YET
          //    value_schema = boost::make_shared<avro::Node>(avro::AVRO_STRING);
          //    break;
        case TIMESTAMPOID:   /* timestamp without time zone: datetime, microseconds since epoch */
          value_schema = std::make_shared<avro::LongSchema>();
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
          value_schema = std::make_shared<avro::BytesSchema>();
          break;
          //case BITOID:     /* fixed-length bit string */
          //case VARBITOID:  /* variable-length bit string */
        case UUIDOID:    /* UUID datatype */
          value_schema = std::make_shared<avro::StringSchema>();
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

          // key value store of string type
        case HSTOREOID: // got 16524 while debugging - is that the only one??
          value_schema = std::make_shared<avro::MapSchema>(avro::StringSchema());
          break;

        case TEXT_ARRAYOID:
          value_schema = std::make_shared<avro::ArraySchema>(avro::StringSchema());
          break;

          /* String-like types: fall through to the default, which is to create a string representation */
        case CHAROID:    /* "char": single character */
        case NAMEOID:    /* name: 63-byte type for storing system identifiers */
        case TEXTOID:    /* text: variable-length string, no limit specified */
          //case BPCHAROID:  /* character(n), char(length): blank-padded string, fixed storage length */
          //case VARCHAROID: /* varchar(length): non-blank-padded string, variable storage length */
          value_schema = std::make_shared<avro::StringSchema>();
          break;

        default:
          LOG(WARNING) << "got unknown OID - mapping to string, OID=" << typid;
          value_schema = std::make_shared<avro::StringSchema>();
          break;
      }

      /* Make a union of value_schema with null. Some types are already a union,
      * in which case they must include null as the first branch of the union,
      * and return directly from the function without getting here (otherwise
      * we'd get a union inside a union, which is not valid Avro). */
      std::shared_ptr<avro::Schema> null_schema = std::make_shared<avro::NullSchema>();
      std::shared_ptr<avro::UnionSchema> union_schema = std::make_shared<avro::UnionSchema>();
      union_schema->addType(*null_schema);
      union_schema->addType(*value_schema);
      //avro_schema_decref(null_schema);
      //avro_schema_decref(value_schema);
      return union_schema;
    }

    /*std::shared_ptr<avro::ValidSchema> schema_for_table_row(std::string schema_name, const PGresult *res) {
      avro::RecordSchema record_schema(schema_name);
      int nFields = PQnfields(res);
      for (int i = 0; i < nFields; i++) {
        Oid col_type = PQftype(res, i);
        std::string col_name = PQfname(res, i);
        std::shared_ptr<avro::Schema> col_schema = schema_for_oid(col_type);
        // TODO ensure that names abide by Avro's requirements
        record_schema.addField(col_name, *col_schema);
      }
      auto result = std::make_shared<avro::ValidSchema>(record_schema);
      return result;
    }
    */

    std::string simple_column_name(std::string column_name) {
      std::string simple = column_name;
      size_t found = simple.find_last_of('.');
      if (found != std::string::npos)
        simple = simple.substr(found + 1);
      return simple;
    }

    // if we have a freeform select statement we might need to specify id and ts columns as a.id and b.ts if the fields occur in several tables
    // strip this away
    std::shared_ptr<avro::ValidSchema>
    schema_for_table_key(std::string schema_name, const std::vector<std::string> &keys, const PGresult *res) {
      avro::RecordSchema record_schema(schema_name);
      for (std::vector<std::string>::const_iterator i = keys.begin(); i != keys.end(); i++) {
        std::string simple_key = simple_column_name(*i);
        int column_index = PQfnumber(res, simple_key.c_str());
        assert(column_index >= 0);
        if (column_index >= 0) {
          Oid col_type = PQftype(res, column_index);
          std::shared_ptr<avro::Schema> col_schema = schema_for_oid(col_type);
          /* TODO ensure that names abide by Avro's requirements */
          record_schema.addField(simple_key, *col_schema);
        }
      }
      auto result = std::make_shared<avro::ValidSchema>(record_schema);
      return result;
    }

//    std::shared_ptr<avro::ValidSchema> schema_for_table_row(std::string schema_name,  const PGresult *res) {
//      avro::RecordSchema record_schema(schema_name);
//      int nFields = PQnfields(res);
//      for (int i = 0; i < nFields; i++) {
//        Oid col_oid = PQftype(res, i);
//        std::string col_name = PQfname(res, i);
//
//        std::shared_ptr<avro::Schema> col_schema;
//
//        auto ext_item = extension_oids_.find(col_oid);
//        if (ext_item != extension_oids_.end())
//          col_schema = ext_item->second;
//        else
//          col_schema = pq::schema_for_oid(col_oid); // build in types
//
//        /* TODO ensure that names abide by Avro's requirements */
//        record_schema.addField(col_name, *col_schema);
//      }
//      auto result = std::make_shared<avro::ValidSchema>(record_schema);
//      return result;
//    }


    // does only support build in types - we have to add a db connection object witrh the extensions to do this right
    std::shared_ptr<avro::ValidSchema> schema_for_table_row(std::string schema_name,  const PGresult *res) {
      avro::RecordSchema record_schema(schema_name);
      int nFields = PQnfields(res);
      for (int i = 0; i < nFields; i++) {
        Oid col_oid = PQftype(res, i);
        std::string col_name = PQfname(res, i);
        std::shared_ptr<avro::Schema> col_schema;
        col_schema = pq::schema_for_oid(col_oid); // build in types
        /* TODO ensure that names abide by Avro's requirements */
        record_schema.addField(col_name, *col_schema);
      }
      auto result = std::make_shared<avro::ValidSchema>(record_schema);
      return result;
    }

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


    std::string avro2sql_table_name(std::shared_ptr<avro::ValidSchema> schema, avro::GenericDatum &datum) {
      auto r = schema->root();
      assert(r->type() == avro::AVRO_RECORD);
      if (r->hasName()) {
        std::string name = r->name().simpleName();

        //since we use convention tablename.key / table_name.value (until we know what bottledwater does...)
        // return namesapace as table name
        std::size_t found = name.find_first_of(".");
        if (found != std::string::npos) {
          std::string ns = name.substr(0, found);
          return ns;
        }
        return r->name().simpleName();
      }
      assert(false);
      return "unknown_table_name";
    }

    std::string avro2sql_column_names(std::shared_ptr<avro::ValidSchema> schema, avro::GenericDatum &datum) {
      auto r = schema->root();
      assert(r->type() == avro::AVRO_RECORD);
      std::string s = "(";
      size_t sz = r->names();
      for (size_t i = 0; i != sz; ++i) {
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


    static Oid avro_type_to_oid(avro::Type avro_type) {
      switch (avro_type) {
        case avro::AVRO_STRING:
          return TEXTOID;
        case avro::AVRO_BYTES:
          return BYTEAOID;
        case avro::AVRO_INT:
          return INT4OID;
        case avro::AVRO_LONG:
          return INT8OID;
        case avro::AVRO_FLOAT:
          return FLOAT4OID;
        case avro::AVRO_DOUBLE:
          return FLOAT8OID;
        case avro::AVRO_BOOL:
          return BOOLOID;

          //case avro::AVRO_ARRAY: // we map arrays to a json string representation of the array eg [ 123, 123 ] or [ "nisse" ]
          //return TEXTOID;
        case avro::AVRO_ARRAY:
          return TEXT_ARRAYOID;

        case avro::AVRO_MAP:
          return HSTOREOID;

        case avro::AVRO_UNION:
        case avro::AVRO_RECORD:
        case avro::AVRO_ENUM:

        case avro::AVRO_FIXED:
        case avro::AVRO_NULL:
        default:
          LOG(FATAL) << "unsupported / non supported type e:" << avro_type;
      }
      return TEXTOID;
    }

    static std::string to_string(Oid oid) {
      switch ((PG_OIDS) oid) {
        case BOOLOID:
          return "boolean";
        case FLOAT4OID:
          return "float4";
        case FLOAT8OID:
          return "float8";
        case INT2OID:
          return "smallint";
        case INT4OID:
          return "integer";
        case INT8OID:
          return "bigint";
        case BYTEAOID:
          return "bytea";
        case CHAROID:
          return "char";
        case NAMEOID:
          return "name";
        case TEXTOID:
          return "text";
        case HSTOREOID:
          return "hstore";
        case TEXT_ARRAYOID:
          return "ARRAY";
        default:
          LOG(FATAL) << "unsupported / non supported type e:" << oid;
          break;
      }
      return "unsupported";
    }

    // this is hackish - if the column is a union type we assume that it can be null
    /*static bool avro2sql_is_column_nullable(const avro::GenericDatum &column) {
      auto t = column.type();
      return (t == avro::AVRO_UNION);
    }
    */

    static std::string keys2string(std::vector<std::string> keys){
      std::string s;
      for (auto j : keys)
        s += j + ","; // (to snanatize_pg_string...)
      if (s.size())
        s.pop_back();
      return s;
    }

    std::string
    avro2sql_create_table_statement(const std::string &tablename, std::vector<std::string> keys, const avro::ValidSchema &schema) {
      auto root = schema.root();
      assert(root->type() == avro::AVRO_RECORD);
      std::string s = "CREATE TABLE " + tablename + " (\n";
      size_t sz = root->names();
      for (size_t i = 0; i != sz; ++i) {
        auto leaf = root->leafAt(i);

        // nullable or not??
        if (leaf->type() == avro::AVRO_UNION) {
          //auto avro_null = leaf->leafAt(0);
          auto avro_val = leaf->leafAt(1);
          s += root->nameAt(i) + " " + to_string(avro_type_to_oid(avro_val->type()));
        } else {
          s += root->nameAt(i) + " " + to_string(avro_type_to_oid(root->leafAt(i)->type())) + " NOT NULL";
        }

        if (i != sz - 1)
          s += ",";
      }

      std::string key_string = keys2string(keys);
      if (key_string.size())
        s += ", PRIMARY KEY(" + key_string + ") ";
      s += ")";
      return s;
    }


    std::string avro2sql_build_insert_1(const std::string &tablename, const avro::ValidSchema &schema) {
      auto r = schema.root();
      assert(r->type() == avro::AVRO_RECORD);
      std::string s = "INSERT INTO " + tablename + "(\n";
      size_t sz = r->names();
      for (size_t i = 0; i != sz; ++i) {
        s += r->nameAt(i);
        if (i != sz - 1)
          s += ",";
      }
      s += ") VALUES\n";
      return s;
    }

    std::string avro2sql_build_upsert_2(const std::string &tablename, const std::vector<std::string> &keys, const avro::ValidSchema &schema) {
      auto r = schema.root();
      assert(r->type() == avro::AVRO_RECORD);
      std::string s = "ON CONFLICT (" + keys2string(keys) + ") DO UPDATE SET (\n";
      size_t sz = r->names();
      for (size_t i = 0; i != sz; ++i) {
        s += r->nameAt(i);
        if (i != sz - 1)
          s += ",";
      }
      s += ") = \n(";

      for (size_t i = 0; i != sz; ++i) {
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
    static std::string avro_2_sql_simple_column_value(const avro::GenericDatum &column) {
      auto t = column.type();
      switch (t) {
        // nullable columns are represented as union of NULL and value
        // parse those recursive
        case avro::AVRO_UNION: {
          LOG(FATAL) << "avro union";
          //const avro::GenericUnion &au(column.value<avro::GenericUnion>());
          //return avro_2_sql_simple_column_value(au.datum());
        }
        case avro::AVRO_NULL:
          return "NULL";
          break;
        case avro::AVRO_STRING: {
          //auto t = column.logicalType();
          std::string s = column.value<std::string>();
          return escapeSQLstring(column.value<std::string>());
        }
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
        case avro::AVRO_ARRAY: {
          const avro::GenericArray &v = column.value<avro::GenericArray>();
          const std::vector<avro::GenericDatum>&r = v.value();
          if (r.size()==0)
            return "'[]'";

          std::vector<avro::GenericDatum>::const_iterator second_last = r.end();
          --second_last;

          std::string s = "[";
          for (std::vector<avro::GenericDatum>::const_iterator i = r.begin(); i!=r.end(); ++i){
            s += avro_2_sql_simple_column_value(*i);
            if (i != second_last)
              s += ", ";
          }
          s += "]";

          return escapeSQLstring(s); // TODO - should we really eascape here - does that not mean that we kill strings in strings since they have ' chars???
        }
          break;

        case avro::AVRO_MAP:{
          const avro::GenericMap &map = column.value<avro::GenericMap>();
          //const std::map<std::string, avro::GenericDatum>&r = map.value();
          auto const &  v = map.value();
          if (v.size()==0)
            return "''";

          avro::GenericMap::Value::const_iterator second_last = v.end();
          --second_last;

          std::string s = "";
          for (avro::GenericMap::Value::const_iterator i = v.begin(); i!=v.end(); ++i){
            // should be '_HOSTIPMI_IP=>10.1.40.23'  ie no inner ''
            //s += i->first + "=>" + avro_2_sql_simple_column_value(i->second);
            // this is a bit inefficient
            std::string value_string = avro_2_sql_simple_column_value(i->second);
            value_string.erase(std::remove_if(value_string.begin(), value_string.end(), IsChars("'")), value_string.end());
            s += i->first + "=>" + value_string;
            if (i != second_last)
              s += ", ";
          }
          s += "";
          return escapeSQLstring(s);
        } break;


        case avro::AVRO_RECORD:
        case avro::AVRO_ENUM:

        case avro::AVRO_FIXED:
        default:
          LOG(FATAL) << "unexpected / non supported type e:" << column.type();
      }
    }

    // handles both nullable and non nullable columns
    std::string avro2sql_values(const avro::ValidSchema &schema, const avro::GenericDatum &datum) {
      std::string result = "(";
      assert(datum.type() == avro::AVRO_RECORD);
      const avro::GenericRecord &record(datum.value<avro::GenericRecord>());
      size_t nFields = record.fieldCount();
      for (size_t i = 0; i < nFields; i++) {
        std::string val = avro_2_sql_simple_column_value(record.fieldAt(i));
        if (i < (nFields - 1))
          result += val + ", ";
        else
          result += val + ")";
      }
      return result;
    }

    // TODO multiple keys
    std::string
    avro2sql_key_values(const avro::ValidSchema &schema, const std::vector<std::string> &keys, const avro::GenericDatum &datum) {
      assert(datum.type() == avro::AVRO_RECORD);
      const avro::GenericRecord &record(datum.value<avro::GenericRecord>());
      std::string result;
      size_t sz = keys.size();
      size_t last =sz-1;
      for (size_t i=0; i!=sz; ++i) {
        auto x = record.field(keys[i]);
        result += avro_2_sql_simple_column_value(x);
        if (i!=last)
          result += ", ";
      }
      return result;
    }

    std::string avro2sql_delete_key_values(const avro::ValidSchema &schema, const std::vector<std::string> &keys,
                                           const avro::GenericDatum &datum) {
      if (datum.type() == avro::AVRO_RECORD) {
        auto root = schema.root();
        assert(root->type() == avro::AVRO_RECORD);
        std::string result = "(";
        const avro::GenericRecord &record(datum.value<avro::GenericRecord>());
        size_t nFields = record.fieldCount();
        for (size_t i = 0; i < nFields; i++) {
          result += root->nameAt(i) + "=";
          std::string val = avro_2_sql_simple_column_value(record.fieldAt(i));
          if (i < (nFields - 1))
            result += "=" + val + " AND ";
          else
            result += "=" + val + ")";
        }
        return result;
      } else {
        if (keys.size()!=1){
          LOG(FATAL) << "keys size!=1 and signal value key";
        }
        return keys[0] + "=" + avro_2_sql_simple_column_value(datum);
      }
    }

    /*std::vector<std::shared_ptr<avro::GenericDatum>> to_avro(std::shared_ptr<avro::ValidSchema> schema, const PGresult *res){

    }
     */

    void load_avro_by_name(kspp::generic_avro* avro, PGresult* pgres, size_t row)
    {
      // key tupe is null if there is no key
      if (avro->type() == avro::AVRO_NULL)
        return;

      assert(avro->type() == avro::AVRO_RECORD);
      avro::GenericRecord& record(avro->generic_datum()->value<avro::GenericRecord>());
      size_t nFields = record.fieldCount();
      for (size_t j = 0; j < nFields; j++)
      {
        avro::GenericDatum& col = record.fieldAt(j); // expected union
        if (!record.fieldAt(j).isUnion()) // this should not hold - but we fail to create correct schemas for not null columns
        {
          LOG(INFO) << avro->valid_schema()->toJson();
          LOG(FATAL) << "unexpected schema - bailing out, type:" << record.fieldAt(j).type();
          break;
        }

        //avro::GenericUnion& au(record.fieldAt(j).value<avro::GenericUnion>());

        const std::string& column_name = record.schema()->nameAt(j);

        //which pg column has this value?
        int column_index = PQfnumber(pgres, column_name.c_str());
        if (column_index < 0)
        {
          LOG(FATAL) << "unknown column - bailing out: " << column_name;
          break;
        }

        if (PQgetisnull(pgres, row, column_index) == 1)
        {
          col.selectBranch(0); // NULL branch - we hope..
          assert(col.type() == avro::AVRO_NULL);
        }
        else
        {
          col.selectBranch(1);
          //au.selectBranch(1);
          //avro::GenericDatum& avro_item(au.datum());
          const char* val = PQgetvalue(pgres, row, j);

          switch (col.type()) {
            case avro::AVRO_STRING:
              col.value<std::string>() = val;
              break;
            case avro::AVRO_BYTES:
              col.value<std::string>() = val;
              break;
            case avro::AVRO_INT:
              col.value<int32_t>() = atoi(val);
              break;
            case avro::AVRO_LONG:
              col.value<int64_t>() = std::stoull(val);
              break;
            case avro::AVRO_FLOAT:
              col.value<float>() = (float) atof(val);
              break;
            case avro::AVRO_DOUBLE:
              col.value<double>() = atof(val);
              break;
            case avro::AVRO_BOOL:
              col.value<bool>() = (val[0] == 't' || val[0] == 'T' || val[0] == '1');
              break;
            case avro::AVRO_MAP: {
              std::vector<std::string> kvs;
              boost::split(kvs, val, boost::is_any_of(",")); // TODO we cannot handle "dsd,hggg" => "jhgf"

              avro::GenericMap& v = col.value<avro::GenericMap>();
              avro::GenericMap::Value& r = v.value();

              // this is an empty string "" that will be mapped as 1 item of empty size
              if (kvs.size()==1 && kvs[0].size() ==0)
                break;

              r.resize(kvs.size());

              int cursor=0;
              for(auto& i : kvs){
                std::size_t found = i.find("=>");
                if (found==std::string::npos)
                  LOG(FATAL) << "expected => in hstore";
                std::string key = i.substr(0, found);
                std::string val = i.substr(found +2);
                pq_trim(key, "\" ");
                pq_trim(val, "\" ");
                r[cursor].first = key;
                r[cursor].second = avro::GenericDatum(val);
                ++cursor;
              }
            }
              break;

            case avro::AVRO_ARRAY:{
              std::vector<std::string> kvs;
              std::string trimmed_val = val;
              pq_trim(trimmed_val, "{ }");
              boost::split(kvs, trimmed_val, boost::is_any_of(",")); // TODO we cannot handle [ "dsd,hg", ljdshf ]
              avro::GenericArray& v = col.value<avro::GenericArray>();
              avro::GenericArray::Value& r = v.value();

              // this is an empty string "" that will be mapped as 1 item of empty size
              if (kvs.size()==1 && kvs[0].size() ==0)
                break;

              r.resize(kvs.size());

              int cursor=0;
              for(auto& i : kvs) {
                r[cursor] = avro::GenericDatum(i);
                ++cursor;
              }

            }
              break;

            case avro::AVRO_RECORD:
            case avro::AVRO_ENUM:
            case avro::AVRO_UNION:
            case avro::AVRO_FIXED:
            case avro::AVRO_NULL:
            default:
              LOG(FATAL) << "unexpected / non supported type e:" << col.type();
          }
        }
      }
    }

  } // namespace
} // namespace