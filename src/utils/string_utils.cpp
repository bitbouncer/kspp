#include <kspp/utils/string_utils.h>
#include <sstream>
#include <iomanip>
#include <exception>
#include <chrono>
#include <boost/algorithm/string.hpp>

namespace kspp {
// see  //https://stackoverflow.com/questions/7724448/simple-json-string-escape-for-c/33799784#33799784
  std::string escape_json(const std::string &s) {
    std::ostringstream o;
    for (auto c = s.cbegin(); c != s.cend(); c++) {
      switch (*c) {
        case '"': o << "\\\""; break;
        case '\\': o << "\\\\"; break;
        case '\b': o << "\\b"; break;
        case '\f': o << "\\f"; break;
        case '\n': o << "\\n"; break;
        case '\r': o << "\\r"; break;
        case '\t': o << "\\t"; break;
        default:
          if ('\x00' <= *c && *c <= '\x1f') {
            o << "\\u"
              << std::hex << std::setw(4) << std::setfill('0') << (int)*c;
          } else {
            o << *c;
          }
      }
    }
    return o.str();
  }

  static inline bool is_bad_sql_char(char c){
    static const std::string chars = "\"'\r\n\t";
    return (chars.find(c) != std::string::npos);
  }

  //USAGE SHOULD BE REPLACED BY PREPARED STATEMENTS
  std::string escape_sql(std::string s){
    s.erase(std::remove_if(s.begin(), s.end(), [](unsigned char x){return is_bad_sql_char(x);}), s.end());
    return s;
  }

  kspp::start_offset_t to_offset(std::string s) {
    if (boost::iequals(s, "OFFSET_BEGINNING"))
      return kspp::OFFSET_BEGINNING;
    else if (boost::iequals(s, "OFFSET_END"))
      return  kspp::OFFSET_END;
    else if (boost::iequals(s, "OFFSET_STORED"))
      return  kspp::OFFSET_STORED;
    throw std::invalid_argument("cannot convert " + s + " to start_offset_t");
  }

  std::string to_string(start_offset_t offset) {
    switch (offset) {
      case kspp::OFFSET_BEGINNING:
        return "OFFSET_BEGINNING";
      case kspp::OFFSET_END:
        return "OFFSET_END";
      case kspp::OFFSET_STORED:
        return "OFFSET_STORED";
      default:
        return std::to_string((int64_t) offset);
    }
  }

  std::chrono::seconds to_duration(std::string s){
    switch (s[s.size()-1]){
      case 'h':
        return std::chrono::seconds(atoi(s.c_str())*3600);
      case 'm':
        return std::chrono::seconds(atoi(s.c_str())*60);
      case 's':
        return std::chrono::seconds(atoi(s.c_str()));
    }
    return std::chrono::seconds(atoi(s.c_str()));
  }

  std::string to_string(std::chrono::seconds s){
    int seconds = s.count();
    if (seconds % 3600 == 0)
      return std::to_string(seconds/3600) + "h";
    if (seconds % 60 == 0)
      return std::to_string(seconds/36) + "m";
    return std::to_string(seconds) + "s";
  }
}

