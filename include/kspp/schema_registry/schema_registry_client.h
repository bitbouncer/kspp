#include <map>
#include <regex>
#include <future>
#include <memory>
#include <avro/Schema.hh>
#include <google/protobuf/descriptor.h>
#include <kspp/utils/http_client.h>
#include <kspp/schema_registry/confluent_http_proxy.h>

#pragma once

namespace kspp {
  class cluster_config;

  class schema_registry_client {
  public:
    schema_registry_client(const kspp::cluster_config &config);
    ~schema_registry_client();
    bool validate();
    int32_t put_schema(std::string subject, std::shared_ptr<const avro::ValidSchema> schema);
    std::shared_ptr<const avro::ValidSchema> get_avro_schema(int32_t schema_id);


    int32_t put_schema(std::string subject, const nlohmann::json& schema);
    nlohmann::json verify_schema(std::string subject, const nlohmann::json& schema);
    std::shared_ptr<const nlohmann::json> get_json_schema(int32_t schema_id);
    std::shared_ptr<const nlohmann::json> get_json_schema(std::string subject);

  private:
    kspp::spinlock spinlock_;
    boost::asio::io_service ios_;
    std::unique_ptr<boost::asio::io_service::work> work_;
    bool fail_fast_;
    std::shared_ptr<confluent_http_proxy> proxy_;
    std::map<int32_t, std::shared_ptr<const avro::ValidSchema>> avro_cache_;
    std::thread thread_;
  };

  nlohmann::json protobuf_register_schema(std::shared_ptr<kspp::schema_registry_client> registry, std::string subject, const google::protobuf::FileDescriptor* file_descriptor);

  template<typename PROTO>
  nlohmann::json protobuf_register_schema(std::shared_ptr<kspp::schema_registry_client> registry, std::string subject){
    //if (std::shared_ptr<const nlohmann::json> schema = registry->get_json_schema(subject))
    //  return *schema;
    PROTO dummy;
    return protobuf_register_schema(registry, subject, dummy.descriptor()->file());
  }
} // kspp