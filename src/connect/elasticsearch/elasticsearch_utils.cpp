#include <kspp/connect/elasticsearch/elasticsearch_utils.h>
#include <glog/logging.h>

#include <avro/Specific.hh>
#include <avro/Encoder.hh>
#include <avro/Decoder.hh>


static std::string avro2elastic_escapeString(std::string src) {
  //we should escape the sql string instead of doing this... - for now this removes ' and " characters in string
  src.erase(std::remove_if(src.begin(), src.end(), avro2elastic_IsChars("'\"")), src.end());
  return src;
  //return std::string("\"" + src + "\""); // we have to do real escaping here to prevent injection attacks  TBD
}

// only maps simple types to value
static std::string avro2elastic_simple_column_value(const avro::GenericDatum &column) {
  auto t = column.type();
  switch (t) {
    // nullable columns are represented as union of NULL and value
    // parse those recursive
    case avro::AVRO_UNION: {
      const avro::GenericUnion& au(column.value<avro::GenericUnion>());
      return avro2elastic_simple_column_value(au.datum());
    }
    case avro::AVRO_NULL:
      return "NULL";
      break;
    case avro::AVRO_STRING:
      return std::string("\"" + avro2elastic_escapeString(column.value<std::string>()) +  "\"");
      break;
    case avro::AVRO_BYTES:
      return std::string("\"" + avro2elastic_escapeString(column.value<std::string>()) +  "\"");
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
      return column.value<bool>() ? "true" : "false";
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
/*
 * std::string avro2elastic_to_json(const avro::ValidSchema& schema, const avro::GenericDatum &datum) {
  auto os = avro::memoryOutputStream();

  avro::EncoderPtr encoder = avro::jsonEncoder(schema);
  encoder->init(*os.get());
  avro::encode(*encoder, datum);
  // push back unused characters to the output stream again... really strange...
  // otherwise content_length will be a multiple of 4096
  encoder->flush();

  auto v = avro::snapshot(*os.get());
  size_t avro_size = v->size();
  std::string res((const char *) v->data(), avro_size);

  return res;
}
 */

/*

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
  */


// handles both nullable and non nullable columns
std::string avro2elastic_json(const avro::ValidSchema& schema, const avro::GenericDatum &datum) {
  auto r = schema.root();
  std::string result = "{";
  assert(datum.type() == avro::AVRO_RECORD);
  const avro::GenericRecord& record(datum.value<avro::GenericRecord>());
  size_t nFields = record.fieldCount();

  bool has_previous=false;
  for (int i = 0; i < nFields; i++) {
    // null columns should nbot be exported to elastic search
    if (record.fieldAt(i).type()!=avro::AVRO_NULL){
      if (has_previous)
        result += ",";
      result += "\"" + r->nameAt(i) + "\":" + avro2elastic_simple_column_value(record.fieldAt(i));
      has_previous=true;
    }
  }
  result +="}";
  return result;
}

// TODO mutiple keys
std::string avro2elastic_key_values(const avro::ValidSchema& schema, const std::string& key, const avro::GenericDatum &datum){
  assert(datum.type() == avro::AVRO_RECORD);
  const avro::GenericRecord& record(datum.value<avro::GenericRecord>());
  std::string result;
  auto x = record.field(key);
  result += avro2elastic_simple_column_value(x);
  return result;
}
