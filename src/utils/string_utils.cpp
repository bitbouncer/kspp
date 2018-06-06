#include <kspp/utils/string_utils.h>
#include <sstream>
#include <iomanip>

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
}
