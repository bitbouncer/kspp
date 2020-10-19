#include <map>
#include <future>
#include <memory>
#include <avro/Schema.hh>
#include <kspp/utils/http_client.h>
#include <kspp/utils/url.h>
#include <kspp/utils/async.h>
#pragma once

namespace kspp {
  class cluster_config;
  /**
   * this is a http client to the confluent schema registry
   */
    class confluent_http_proxy {
      struct rpc_put_schema_result {
        rpc_put_schema_result() : ec(-1), schema_id(-1) {}

        int ec;
        int32_t schema_id;
      };

      struct rpc_get_result {
        rpc_get_result() : ec(-1) {
        }

        int ec;
        std::shared_ptr<avro::ValidSchema> schema;
      };

      struct rpc_get_config_result {
        rpc_get_config_result() : ec(-1) {
        }
        int ec;
        std::string config;
      };

    public:
      typedef std::function<void(rpc_put_schema_result)> put_callback;
      typedef std::function<void(rpc_get_result)> get_callback;
      typedef std::function<void(rpc_get_config_result)> get_top_level_config_callback;

      confluent_http_proxy(boost::asio::io_service &ios, const kspp::cluster_config& config);

      void get_config(get_top_level_config_callback cb);

      std::future<rpc_get_config_result> get_config() {
        auto p = std::make_shared<std::promise<rpc_get_config_result>>();
        get_config([p](rpc_get_config_result result) {
          p->set_value(result);
        });
        return p->get_future();
      }

      //void close(); // remove
      void put_schema(std::string name, std::shared_ptr<const avro::ValidSchema>, put_callback);

      std::future<rpc_put_schema_result> put_schema(std::string name, std::shared_ptr<const avro::ValidSchema> schema) {
        auto p = std::make_shared<std::promise<rpc_put_schema_result>>();
        put_schema(name, schema, [p](rpc_put_schema_result result) {
          p->set_value(result);
        });
        return p->get_future();
      }

      void get_schema(int32_t id, get_callback);

      std::future<rpc_get_result> get_schema(int32_t schema_id) {
        auto p = std::make_shared<std::promise<rpc_get_result>>();
        get_schema(schema_id, [p](rpc_get_result result) {
          p->set_value(result);
        });
        return p->get_future();
      }

    private:
      kspp::http::client _http;
      kspp::async::scheduling_t _read_policy;
      std::chrono::milliseconds _http_timeout;
      std::vector<kspp::url> _base_urls;
      const std::string _ca_cert_path;
      const std::string _client_cert_path;
      const std::string _private_key_path;
      const std::string _private_key_passphrase;
      bool _verify_host;
      std::map<int32_t, std::shared_ptr<const avro::ValidSchema>> _registry;
    };
} // kspp