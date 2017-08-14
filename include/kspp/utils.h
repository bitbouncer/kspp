#include <cstdlib>
#include <boost/filesystem.hpp>
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

    inline boost::filesystem::path default_statestore_directory() {
      if (const char *env_p = std::getenv("KSPP_STATE_DIR")) {
        return boost::filesystem::path(env_p);
      }
      return boost::filesystem::temp_directory_path();
    }
  }
}

