#include <kspp/utils/env.h>
#include <experimental/filesystem>
#include <boost/asio/ip/host_name.hpp>
#include <glog/logging.h>

namespace kspp {
  std::string get_env_and_log(const char *env, std::string default_value) {
    const char *env_p = std::getenv(env);
    if (env_p) {
      LOG(INFO) << "env: " << env << " -> " << env_p;
      return std::string(env_p);
    } else {
      if (default_value.size())
        LOG(INFO) << "env: " << env << " - not defined, using default: " << default_value;
      else
        LOG(INFO) << "env: " << env << " - not defined";
      return default_value;
    }
  }

  std::string get_env_and_log_hidden(const char *env, std::string default_value) {
    const char *env_p = std::getenv(env);
    if (env_p) {
      LOG(INFO) << "env: " << env << " -> [hidden]";
      return std::string(env_p);
    } else {
      LOG(INFO) << "env: " << env << " - not defined, using default: [hidden]";
      return default_value;
    }
  }

  std::string default_kafka_broker_uri() {
    return get_env_and_log("KSPP_KAFKA_BROKER_URL", "plaintext://localhost:9092");
  }

  std::string default_kafka_rest_uri() {
    return get_env_and_log("KSPP_KAFKA_REST_URL", "http://localhost:8082");
  }

  std::string default_schema_registry_uri() {
    return get_env_and_log("KSPP_SCHEMA_REGISTRY_URL", "http://localhost:8081");
  }

  std::string default_statestore_root() {
    return get_env_and_log("KSPP_STATE_STORE_ROOT",
                           std::experimental::filesystem::temp_directory_path().generic_string() + "/kspp");
  }

#ifdef _WIN32
  std::string default_ca_cert_path() {
    return get_env_and_log("KSPP_CA_CERT", "");
  }

  std::string default_client_cert_path() {
    return get_env_and_log("KSPP_CLIENT_CERT", "");
  }

  std::string default_client_key_path() {
    return get_env_and_log("KSPP_CLIENT_KEY", "");
  }

  std::string default_client_key_passphrase() {
    return get_env_and_log_hidden("KSPP_CLIENT_KEY_PASSPHRASE", "");
  }
#else

  std::string default_pushgateway_uri() {
    return get_env_and_log("KSPP_PUSHGATEWAY_URL", "http://localhost:9091");
  }

  std::string default_ca_cert_path() {
    return get_env_and_log("KSPP_CA_CERT", "/etc/kspp/credentials/cacert.pem");
  }

  std::string default_client_cert_path() {
    return get_env_and_log("KSPP_CLIENT_CERT", "/etc/kspp/credentials/client.pem");
  }

  std::string default_client_key_path() {
    return get_env_and_log("KSPP_CLIENT_KEY", "/etc/kspp/credentials/client.key");
  }

  std::string default_client_key_passphrase() {
    return get_env_and_log_hidden("KSPP_CLIENT_KEY_PASSPHRASE", "");
  }

#endif

  /*
   * std::string default_hostname() {
#ifdef _WIN32
    if (const char* env_p = std::getenv("COMPUTERNAME"))
      return std::string(env_p);
    else
      return "unknown";
#else
    char buf[256];
    sprintf(buf, "unknown");
    gethostname(buf, 256);
    std::string hostname = buf;
    return hostname;
#endif
  }
  */
  std::string default_hostname() {
    auto host_name = boost::asio::ip::host_name();
    return host_name;
  }
}

