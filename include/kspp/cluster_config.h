#include <string>
#include <chrono>
#pragma once

namespace kspp {
  class cluster_config {
  public:
    cluster_config();
    void set_brokers(std::string uri);
    void set_storage_root(std::string s);
    void set_ca_cert_path(std::string path);
    void set_private_key_path(std::string cert_path, std::string key_path, std::string passprase="");
    void set_schema_registry(std::string);
    void set_consumer_buffering_time(std::chrono::milliseconds timeout);
    void set_producer_buffering_time(std::chrono::milliseconds timeout);


    std::string get_brokers() const {
      return brokers_;
    }

    std::string get_ca_cert_path() const {
      return ca_cert_path_;
    }

    std::string get_client_cert_path() const {
      return client_cert_path_;
    }

    std::string get_private_key_path() const {
      return private_key_path_;
    }

    std::string get_private_key_passphrase() const {
      return private_key_passphrase_;
    }

    std::chrono::milliseconds get_producer_buffering_time() const {
      return producer_buffering_;
    }

    std::chrono::milliseconds get_consumer_buffering_time() const {
      return conumer_buffering_;
    }

    std::string get_storage_root() const {
      return root_path_;
    }


    void validate() const;

  private:
    std::string brokers_;
    std::string ca_cert_path_;
    std::string client_cert_path_;
    std::string private_key_path_;
    std::string private_key_passphrase_;
    std::chrono::milliseconds producer_buffering_;
    std::chrono::milliseconds conumer_buffering_;
    std::string root_path_;
    std::string schema_registry_uri_;
  };
}