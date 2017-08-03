#include <map>
#include <avro/Schema.hh>
#include <csi_hcl_asio/http_client.h>
#pragma once

namespace confluent
{
  class registry
	{
    
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

    struct rpc_put_schema_result
    {
      rpc_put_schema_result() : ec(-1), schema_id(-1) {}
      int     ec;
      int32_t schema_id;
    };


    struct rpc_get_result
    {
      rpc_get_result() : ec(-1) {
      }
      int                                ec;
      std::shared_ptr<avro::ValidSchema> schema;
    };

	public:
		typedef std::function <void(rpc_put_schema_result)>  put_callback;
		typedef std::function <void(rpc_get_result)>         get_callback;

		registry(boost::asio::io_service& ios, std::vector<std::string> base_urls); // how to give a vector??
    //void close(); // remove
		void put_schema(std::string name, std::shared_ptr<avro::ValidSchema>, put_callback);

    std::future<rpc_put_schema_result> put_schema(std::string name, std::shared_ptr<avro::ValidSchema> schema) {
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

		//inline boost::asio::io_service& ios() { return _ios; }
		//int32_t get_cached_schema(std::shared_ptr<avro::ValidSchema>);
		//std::shared_ptr<avro::ValidSchema> get_schema_by_id(int32_t id);

	private:
		//boost::asio::io_service&					                			_ios;
		csi::http::client                                       _http;
    std::vector<std::string>                                _base_urls;
		std::map<int32_t, boost::shared_ptr<avro::ValidSchema>> _registry;
	};
};

