#include <avro/Generic.hh>
#pragma once

namespace kspp
{
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
}