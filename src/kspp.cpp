#include <kspp/kspp.h>
namespace kspp {
  std::string to_string(start_offset_t offset){
    switch (offset){
      case kspp::OFFSET_BEGINNING: return "OFFSET_BEGINNING";
      case kspp::OFFSET_END: return "OFFSET_END";
      case kspp::OFFSET_STORED: return "OFFSET_STORED";
      default:
        return std::to_string((int64_t) offset);
    };
  }
}

