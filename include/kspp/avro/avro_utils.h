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
}