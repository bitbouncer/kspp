#include <string>

#pragma once
namespace kspp {
  std::string default_kafka_broker_uri();

  std::string default_kafka_rest_uri();

  std::string default_schema_registry_uri();

  std::string default_statestore_root();

  std::string default_ca_cert_path();

  std::string default_client_cert_path();

  std::string default_client_key_path();

  std::string default_client_key_passphrase();

  std::string default_hostname();

  std::string get_env_and_log(const char* env, std::string default_value="");

  std::string get_env_and_log_hidden(const char* env, std::string default_value="");
}

