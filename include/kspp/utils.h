#include <cstdlib>
#pragma once

namespace kspp
{
  namespace utils
  {
    inline std::string default_kafka_broker_uri() {
      if (const char* env_p = std::getenv("KAFKA_BROKER_URI"))
        return std::string(env_p);
      return std::string("plaintext://localhost:9092");
    }

      inline std::string default_schema_registry_uri() {
      if (const char* env_p = std::getenv("CONFLUENT_SCHEMA_REGISTRY_URI"))
        return std::string(env_p);
      return std::string("http://localhost:8081");
    }
  }
}

