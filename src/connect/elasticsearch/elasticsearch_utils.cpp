#include <kspp/connect/elasticsearch/elasticsearch_utils.h>
#include <glog/logging.h>
#include <avro/Specific.hh>
#include <avro/Encoder.hh>
#include <avro/Decoder.hh>
#include <kspp/utils/string_utils.h>

namespace kspp {
// only maps simple types to value
  std::string avro2elastic_simple_column_value(const avro::GenericDatum &column) {
    auto t = column.type();
    switch (t) {
      // nullable columns are represented as union of NULL and value
      // parse those recursive
      case avro::AVRO_UNION: {
        const avro::GenericUnion &au(column.value<avro::GenericUnion>());
        return avro2elastic_simple_column_value(au.datum());
      }
      case avro::AVRO_NULL:
        return "NULL";
        break;
      case avro::AVRO_STRING:
        return std::string("\"" + escape_json(column.value<std::string>()) + "\"");
        break;
      case avro::AVRO_BYTES:
        return std::string("\"" + escape_json(column.value<std::string>()) + "\"");
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

  std::string avro_simple_column_value(const avro::GenericDatum &column) {
    auto t = column.type();
    switch (t) {
      // nullable columns are represented as union of NULL and value
      // parse those recursive
      case avro::AVRO_UNION: {
        const avro::GenericUnion &au(column.value<avro::GenericUnion>());
        return avro2elastic_simple_column_value(au.datum());
      }
      case avro::AVRO_NULL:
        return "NULL";
        break;
      case avro::AVRO_STRING:
        return std::string(column.value<std::string>());
        break;
      case avro::AVRO_BYTES:
        return std::string(column.value<std::string>());
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

  static avro::Type avro2elastic_simple_column_type(const avro::GenericDatum &column) {
    auto t = column.type();
    // nullable columns are represented as union of NULL and value
    // parse those recursive
    if (t == avro::AVRO_UNION) {
      const avro::GenericUnion &au(column.value<avro::GenericUnion>());
      return avro2elastic_simple_column_type(au.datum());
    } else {
      return t;
    }
  }

// handles both nullable and non nullable columns
  std::string avro2elastic_json(const avro::ValidSchema &schema, const avro::GenericDatum &datum) {
    auto r = schema.root();
    std::string result = "{";
    assert(datum.type() == avro::AVRO_RECORD);
    const avro::GenericRecord &record(datum.value<avro::GenericRecord>());
    size_t nFields = record.fieldCount();

    bool has_previous = false;
    for (int i = 0; i < nFields; i++) {
      // null columns should nbot be exported to elastic search
      if (avro2elastic_simple_column_type(record.fieldAt(i)) != avro::AVRO_NULL) {
        if (has_previous)
          result += ", ";
        result += "\"" + r->nameAt(i) + "\": " + avro2elastic_simple_column_value(record.fieldAt(i));
        has_previous = true;
      }
    }
    result += "}";
    return result;
  }

// TODO mutiple keys
  std::string avro2elastic_key_values(const avro::ValidSchema &schema, const std::string &key, const avro::GenericDatum &datum) {
    assert(datum.type() == avro::AVRO_RECORD);
    const avro::GenericRecord &record(datum.value<avro::GenericRecord>());
    std::string result;
    auto x = record.field(key);
    result += avro2elastic_simple_column_value(x);
    return result;
  }
} // namespace