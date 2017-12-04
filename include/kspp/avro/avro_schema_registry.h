#include <map>
#include <regex>
#include <future>
#include <memory>
#include <avro/Schema.hh>
#include <kspp/utils/http_client.h>
#include <kspp/avro/confluent_http_proxy.h>
#pragma once

namespace kspp {
  class cluster_config;
  class avro_schema_registry {
  public:
    avro_schema_registry(const kspp::cluster_config& config);
    ~avro_schema_registry();
    bool validate();
    int32_t put_schema(std::string name, std::shared_ptr<const avro::ValidSchema> schema);
    std::shared_ptr<const avro::ValidSchema> get_schema(int32_t schema_id);
  private:
    boost::asio::io_service _ios;
    bool _fail_fast;
    std::unique_ptr<boost::asio::io_service::work> _work;
    std::thread _thread;
    std::shared_ptr<confluent_http_proxy> _proxy;
    kspp::spinlock _spinlock;
    std::map<int32_t, std::shared_ptr<const avro::ValidSchema>> _cache;
  };
} // kspp