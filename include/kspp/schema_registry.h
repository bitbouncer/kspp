#include <map>
#include <regex>
#include <avro/Schema.hh>
#include <kspp/impl/http/http_client.h>
#pragma once

namespace kspp {

  /**
   * this is a http client to the confluent schema registry
   */
  namespace confluent {
    class registry {

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

    public:
      typedef std::function<void(rpc_put_schema_result)> put_callback;
      typedef std::function<void(rpc_get_result)> get_callback;

      registry(boost::asio::io_service &ios, std::vector<std::string> base_urls); // how to give a vector??
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
      std::vector<std::string> _base_urls;
      std::map<int32_t, boost::shared_ptr<const avro::ValidSchema>> _registry;
    };
  };


  class avro_schema_registry
  {
  public:
    avro_schema_registry(std::string urls)
        : _work(new boost::asio::io_service::work(_ios))
        , _thread([this] { _ios.run(); })
        , _registry(std::make_shared<confluent::registry>(_ios, split_urls(urls))) {}

    ~avro_schema_registry() {
      _registry.reset();
      _work.reset();
      _thread.join();
    }

    int32_t put_schema(std::string name, std::shared_ptr<const avro::ValidSchema> schema) {
      auto future = _registry->put_schema(name, schema);
      future.wait();
      auto rpc_result = future.get();
      if (rpc_result.ec) {
        LOG(ERROR) << "schema_registry put failed: ec" << rpc_result.ec;
        return -1;
      }

      LOG(INFO) << "schema_registry put OK: id" << rpc_result.schema_id;
      return rpc_result.schema_id;
    }

    std::shared_ptr<const avro::ValidSchema> get_schema(int32_t schema_id) {
      //TBD mutex
      std::map<int32_t, std::shared_ptr<const avro::ValidSchema>>::iterator item = _cache.find(schema_id);
      if (item != _cache.end())
        return item->second;

      auto future = _registry->get_schema(schema_id);
      future.wait();
      auto rpc_result = future.get();
      if (rpc_result.ec) {
        LOG(ERROR) << "schema_registry get failed: ec" << rpc_result.ec;
        return nullptr;
      }

      //TBD mutex
      _cache[schema_id] = rpc_result.schema;
      return rpc_result.schema;
    }

  private:
    static std::vector<std::string> split_urls(std::string s) {
      std::vector<std::string> result;
      std::regex rgx("[,\\s+]");
      std::sregex_token_iterator iter(s.begin(), s.end(), rgx, -1);
      std::sregex_token_iterator end;
      for (; iter != end; ++iter)
        result.push_back(*iter);
      return result;
    }

    boost::asio::io_service                               _ios;
    std::unique_ptr<boost::asio::io_service::work>        _work;
    std::thread                                           _thread;
    std::shared_ptr<confluent::registry>                  _registry;
    std::map<int32_t, std::shared_ptr<const avro::ValidSchema>> _cache;
  };
}; // kspp