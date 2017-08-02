#include <cstdlib>
#pragma once

namespace kspp
{
  namespace utils
  {
    inline std::string default_kafka_broker() {
      if (const char* env_p = std::getenv("KAFKA_BROKER"))
        return std::string(env_p);
      return "localhost";
    }
  }
}

