#include <avro/Generic.hh>
#include <avro/Compiler.hh>
#include <boost/uuid/uuid.hpp>
#pragma once

namespace kspp
{
  std::string normalize(const avro::ValidSchema &vs);
  std::string to_string(avro::Type t);

// not implemented for
// AVRO_NULL (not relevant)
// AVRO_RECORD
// AVRO_ENUM  - dont know how...
// AVRO_ARRAY - dont know how...
// AVRO_MAP - dont know how...
// AVRO_UNION
// AVRO_FIXED

  template<typename T> avro::Type cpp_to_avro_type();
  template<> inline avro::Type cpp_to_avro_type<std::string>() { return avro::AVRO_STRING; }
  template<> inline avro::Type cpp_to_avro_type<std::vector<uint8_t>>() { return avro::AVRO_BYTES; }
  template<> inline avro::Type cpp_to_avro_type<int32_t>() { return avro::AVRO_INT; }
  template<> inline avro::Type cpp_to_avro_type<int64_t>() { return avro::AVRO_LONG; }
  template<> inline avro::Type cpp_to_avro_type<float>() { return avro::AVRO_FLOAT; }
  template<> inline avro::Type cpp_to_avro_type<double>() { return avro::AVRO_DOUBLE; }
  template<> inline avro::Type cpp_to_avro_type<bool>() { return avro::AVRO_BOOL; }
  template<> inline avro::Type cpp_to_avro_type<avro::GenericArray>() { return avro::AVRO_ARRAY; }
  template<> inline avro::Type cpp_to_avro_type<avro::GenericRecord>() { return avro::AVRO_RECORD; }

  template<typename T> T convert(const avro::GenericDatum& datum){
    if (cpp_to_avro_type<T>() == datum.type())
      return datum.value<T>();
    throw std::invalid_argument(std::string("avro convert: wrong type, expected: [") + to_string(cpp_to_avro_type<T>()) +  "], actual: " + to_string(datum.type()));
  }

  template<> inline int64_t convert(const avro::GenericDatum& datum){
    switch (datum.type()) {
      case avro::AVRO_LONG:
        return datum.value<int64_t>();
      case avro::AVRO_INT:
        return datum.value<int32_t>();
      default:
        throw std::invalid_argument(std::string("avro convert: wrong type, expected: [LONG, INT], actual: ") + to_string(datum.type()));
    }
  }


  template<typename T>
  class avro_utils{
  public:
    static std::string schema_name(const T& dummy){
      return T::avro_schema_name();
    }

    // default implementation uses  kspp_avrogen gebnerated stuff - we could generated this function instead in avrogen cpp???
    static std::string schema_as_string(const T& dummy){
      return T::schema_as_string();
    }

    static std::shared_ptr<const avro::ValidSchema> valid_schema(const T& dummy) {
      static const std::shared_ptr<const ::avro::ValidSchema> _validSchema(std::make_shared<const ::avro::ValidSchema>(
          ::avro::compileJsonSchemaFromString(schema_as_string(dummy))));
      return _validSchema;
    }
  };

  template<>
  inline std::string avro_utils<std::string>::schema_name(const std::string& dummy){ return "string"; }

  template<>
  inline std::string avro_utils<std::string>::schema_as_string(const std::string& dummy){ return "{\"type\":\"string\"}"; }


  template<>
  inline std::string avro_utils<int64_t>::schema_name(const int64_t& dummy){ return "long"; }

  template<>
  inline std::string avro_utils<int64_t>::schema_as_string(const int64_t& dummy){ return "{\"type\":\"long\"}"; }


  template<>
  inline std::string avro_utils<int32_t>::schema_name(const int32_t& dummy){ return "int"; }

  template<>
  inline std::string avro_utils<int32_t>::schema_as_string(const int32_t& dummy){ return "{\"type\":\"int\"}"; }


  template<>
  inline std::string avro_utils<bool>::schema_name(const bool& dummy){ return "boolean"; }

  template<>
  inline std::string avro_utils<bool>::schema_as_string(const bool& dummy){ return "{\"type\":\"boolean\"}"; }

  template<>
  inline std::string avro_utils<float>::schema_name(const float& dummy){ return "float"; }

  template<>
  inline std::string avro_utils<float>::schema_as_string(const float& dummy){ return "{\"type\":\"float\"}"; }

  template<>
  inline std::string avro_utils<double>::schema_name(const double& dummy){ return "double"; }

  template<>
  inline std::string avro_utils<double>::schema_as_string(const double& dummy){ return "{\"type\":\"double\"}"; }

  template<>
  inline std::string avro_utils<std::vector<uint8_t>>::schema_name(const std::vector<uint8_t>& dummy){ return "bytes"; }

  template<>
  inline std::string avro_utils<std::vector<uint8_t>>::schema_as_string(const std::vector<uint8_t>& dummy){ return "{\"type\":\"bytes\"}"; }

  template<>
  inline std::string avro_utils<boost::uuids::uuid>::schema_name(const boost::uuids::uuid& dummy){ return "uuid"; }

  template<>
  inline std::string avro_utils<boost::uuids::uuid>::schema_as_string(const  boost::uuids::uuid& dummy){ return "{\"type\":\"string\"}"; }
} // namespace
