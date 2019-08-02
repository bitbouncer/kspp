#include <string>
#include <chrono>
#include <mutex>
#include <memory>
#include <kspp/avro/avro_serdes.h>
#pragma once

namespace kspp {
  class cluster_metadata;
  class avro_schema_registry;

  class cluster_config {
  public:
    enum flags_t { NONE=0x0, KAFKA=0x01, SCHEMA_REGISTRY=0x02, PUSHGATEWAY=0x04, BB_STREAMING=0x08 };

    cluster_config(std::string consumer_group, uint64_t flags = KAFKA | SCHEMA_REGISTRY | PUSHGATEWAY );

    inline bool has_feature(flags_t f) const {  return (flags_ & f); }

    void load_config_from_env();

    void set_brokers(std::string uri);
    std::string get_brokers() const;

    std::string get_consumer_group() const;

    void set_consumer_buffering_time(std::chrono::milliseconds timeout);
    std::chrono::milliseconds get_consumer_buffering_time() const;

    void set_producer_buffering_time(std::chrono::milliseconds timeout);
    std::chrono::milliseconds get_producer_buffering_time() const;

    void set_producer_message_timeout(std::chrono::milliseconds timeout);
    std::chrono::milliseconds get_producer_message_timeout() const;

    void set_min_topology_buffering(std::chrono::milliseconds timeout);
    std::chrono::milliseconds get_min_topology_buffering() const;

    void set_max_pending_sink_messages(size_t sz);
    size_t get_max_pending_sink_messages() const;

    bool set_ca_cert_path(std::string path);
    std::string get_ca_cert_path() const;

    void set_private_key_path(std::string cert_path, std::string key_path, std::string passphrase="");
    std::string get_client_cert_path() const;
    std::string get_private_key_path() const;
    std::string get_private_key_passphrase() const;

    void set_schema_registry_uri(std::string);
    std::string get_schema_registry_uri() const;

    //void set_kafka_rest_uri(std::string);
    //std::string get_kafka_rest_uri() const;

    void set_pushgateway_uri(std::string);
    std::string get_pushgateway_uri() const;

    void set_schema_registry_timeout(std::chrono::milliseconds timeout);
    std::chrono::milliseconds get_schema_registry_timeout() const;

    void set_storage_root(std::string s);
    std::string get_storage_root() const;

    void set_fail_fast(bool state);
    bool get_fail_fast() const;

    std::shared_ptr<cluster_metadata> get_cluster_metadata() const;

    void set_cluster_state_timeout(std::chrono::seconds);
    std::chrono::seconds get_cluster_state_timeout() const ;

    std::shared_ptr<kspp::avro_serdes> avro_serdes(bool relaxed_parsing=false);

    std::shared_ptr<kspp::avro_schema_registry> get_schema_registry(){
      if (!avro_schema_registry_)
        avro_schema_registry_ =  std::make_shared<kspp::avro_schema_registry>(*this);
      return avro_schema_registry_;
    }

    void validate();

    void log() const;

  private:
    uint64_t    flags_;
    std::string consumer_group_;
    std::string brokers_;
    std::string ca_cert_path_;
    std::string client_cert_path_;
    std::string private_key_path_;
    std::string private_key_passphrase_;
    std::chrono::milliseconds min_topology_buffering_;
    std::chrono::milliseconds producer_buffering_;
    std::chrono::milliseconds producer_message_timeout_;
    std::chrono::milliseconds consumer_buffering_;
    std::chrono::milliseconds schema_registry_timeout_;
    std::chrono::seconds cluster_state_timeout_;
    size_t max_pending_sink_messages_;
    std::string root_path_;
    std::string schema_registry_uri_;
    std::string pushgateway_uri_;

    bool fail_fast_;
    mutable std::shared_ptr<cluster_metadata> meta_data_;
    mutable std::shared_ptr<kspp::avro_schema_registry> avro_schema_registry_;
    mutable std::shared_ptr<kspp::avro_serdes> avro_serdes_;
  };
}