#include <avro/Generic.hh>
#include <avro/Schema.hh>
#include <kspp/avro/generic_avro.h>

#pragma once

namespace kspp {
  // decorated as json - ie "" around strings and [] around arrays
  std::string avro_2_json_simple_column_value(const avro::GenericDatum &datum);

  //no decoration
  std::string avro_2_raw_column_value(const avro::GenericDatum &column);

  std::string
  avro2elastic_key_values(const avro::ValidSchema &schema, const std::string &key, const avro::GenericDatum &datum);

  std::string avro2elastic_json(const avro::ValidSchema &schema, const avro::GenericDatum &datum);

  class avro2elastic_IsChars {
  public:
    avro2elastic_IsChars(const char *charsToRemove) : chars(charsToRemove) {};

    inline bool operator()(char c) {
      for (const char *testChar = chars; *testChar != 0; ++testChar) {
        if (*testChar == c) { return true; }
      }
      return false;
    }

  private:
    const char *chars;
  };
}
