#include <string>
#include <algorithm>
#include <chrono>
#include <kspp/typedefs.h>
#pragma once

namespace kspp {
  std::string escape_json(const std::string &s);

  inline void ltrim(std::string &s) {
    s.erase(s.begin(), std::find_if(s.begin(), s.end(), std::not1(std::ptr_fun<int, int>(std::isspace))));
  }

// trim from end (in place)
  inline void rtrim(std::string &s) {
    s.erase(std::find_if(s.rbegin(), s.rend(), std::not1(std::ptr_fun<int, int>(std::isspace))).base(), s.end());
  }

// trim from both ends (in place)
  inline void trim(std::string &s) {
    ltrim(s);
    rtrim(s);
  }

  kspp::start_offset_t to_offset(std::string);
  std::string to_string(start_offset_t);

  std::chrono::seconds to_duration(std::string s);
  std::string to_string(std::chrono::seconds s);
}
