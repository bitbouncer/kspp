#include <map>
#include <future>
#include <memory>
#include <avro/Schema.hh>
#include <kspp/utils/http_client.h>
#include <kspp/cluster_config.h>
#include <kspp/utils/url.h>
#pragma once

namespace kspp {
  /**
   * this is a http client to the confluent schema registry
   */
    class confluent_http_proxy {

      // ?? change to schema registry rpc_result and result -> schema_id if we always have that type...
      // maybee add name to result as well...
      /*
      template <class result_type>
      struct rpc_result
      {
        rpc_result() : ec(-1), result(-1) {}
        int         ec;
        result_type result;
      };
      */

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

      confluent_http_proxy(boost::asio::io_service &ios, std::shared_ptr<kspp::cluster_config> config); // how to give a vector??

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
      std::shared_ptr<kspp::cluster_config> _config;
      kspp::http::client _http;
      std::chrono::milliseconds _http_timeout;
      std::vector<kspp::url> _base_urls;
      std::map<int32_t, boost::shared_ptr<const avro::ValidSchema>> _registry;
    };
} // kspp