#include <sstream>
#include "boost/any.hpp"
#include "avro/Specific.hh"
#include "avro/Encoder.hh"
#include "avro/Decoder.hh"
#include "avro/Compiler.hh"
#pragma once

namespace metrics20 {
namespace avro {
struct metrics20_key_tags_t {
  std::string key;
  std::string value;
  metrics20_key_tags_t() :
    key(std::string()),
    value(std::string()){
  }

};

struct metrics20_key_t {
  std::vector<metrics20_key_tags_t> tags;
  metrics20_key_t() :
    tags(std::vector<metrics20_key_tags_t>()){
  }

  //returns the string representation of the schema of self (avro extension for kspp avro serdes)
  static inline const char* schema_as_string() {
    return "{\"type\":\"record\",\"namespace\":\"metrics20.avro\",\"name\":\"metrics20_key_t\",\"fields\":[{\"name\":\"tags\",\"type\":{\"type\":\"array\",\"items\":{\"type\":\"record\",\"namespace\":\"metrics20.avro\",\"name\":\"metrics20_key_tags_t\",\"fields\":[{\"name\":\"key\",\"type\":\"string\"},{\"name\":\"value\",\"type\":\"string\"}]}}}]}";
  } 

  //returns a valid schema of self (avro extension for kspp avro serdes)
  static std::shared_ptr<const ::avro::ValidSchema> valid_schema() {
    static const std::shared_ptr<const ::avro::ValidSchema> _validSchema(std::make_shared<const ::avro::ValidSchema>(::avro::compileJsonSchemaFromString(schema_as_string())));
    return _validSchema;
  }

  //returns the (type)name of self (avro extension for kspp avro serdes)
  static std::string avro_schema_name(){
    return "metrics20.avro.metrics20_key_t";
  }
};

} // namespace metrics20
} // namespace avro

namespace avro {
template<> struct codec_traits<metrics20::avro::metrics20_key_tags_t> {
  static void encode(Encoder& e, const metrics20::avro::metrics20_key_tags_t& v) {
    avro::encode(e, v.key);
    avro::encode(e, v.value);
  }
  static void decode(Decoder& d, metrics20::avro::metrics20_key_tags_t& v) {
    if (avro::ResolvingDecoder *rd =
      dynamic_cast<avro::ResolvingDecoder *>(&d)) {
        const std::vector<size_t> fo = rd->fieldOrder();
        for (std::vector<size_t>::const_iterator it = fo.begin(); it != fo.end(); ++it) {
          switch (*it) {
          case 0:
          avro::decode(d, v.key);
          break;
          case 1:
          avro::decode(d, v.value);
          break;
            default:
            break;
          }
        }
    } else {
      avro::decode(d, v.key);
      avro::decode(d, v.value);
    }
  }
};

template<> struct codec_traits<metrics20::avro::metrics20_key_t> {
  static void encode(Encoder& e, const metrics20::avro::metrics20_key_t& v) {
    avro::encode(e, v.tags);
  }
  static void decode(Decoder& d, metrics20::avro::metrics20_key_t& v) {
    if (avro::ResolvingDecoder *rd =
      dynamic_cast<avro::ResolvingDecoder *>(&d)) {
        const std::vector<size_t> fo = rd->fieldOrder();
        for (std::vector<size_t>::const_iterator it = fo.begin(); it != fo.end(); ++it) {
          switch (*it) {
          case 0:
          avro::decode(d, v.tags);
          break;
            default:
            break;
          }
        }
    } else {
      avro::decode(d, v.tags);
    }
  }
};

}
