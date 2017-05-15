#include <rapidjson/document.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/prettywriter.h>

#include "confluent_schema_registry.h"
#include <future>
#include <memory>
#include <boost/log/core.hpp>
#include <boost/log/trivial.hpp>
#include <boost/log/expressions.hpp>
#include <avro/Compiler.hh>
#include <csi-async/async.h>
#include <csi_hcl_asio/http_client.h>

using namespace std::chrono_literals;

namespace confluent {
static inline std::string normalize(const avro::ValidSchema& vs) {
  std::stringstream ss;
  vs.toJson(ss);
  std::string s = ss.str();
  // TBD we should strip type : string to string 
  // strip whitespace
  s.erase(remove_if(s.begin(), s.end(), ::isspace), s.end());  // c version does not use locale... 
  return s;
}

static inline void add_member(std::shared_ptr<rapidjson::Document> document, std::string key, std::string value) {
  rapidjson::Document::AllocatorType& allocator = document->GetAllocator();
  rapidjson::Value ks;
  rapidjson::Value vs;
  ks.SetString(key.c_str(), allocator);
  vs.SetString(value.c_str(), allocator);
  document->AddMember(ks, vs, allocator);
}

std::string encode_put_schema_request(std::shared_ptr<avro::ValidSchema> schema) {
  auto s = normalize(*schema);
  auto document = std::make_shared<rapidjson::Document>();
  document->SetObject();
  add_member(document, "schema", s);
  rapidjson::StringBuffer buffer;
  rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
  document->Accept(writer);
  return buffer.GetString();
}

bool decode_put_schema_request_response(char* buf, size_t len, int32_t* result) {
  auto document = std::make_shared<rapidjson::Document>();
  document->Parse(buf, len);
  if (document->HasParseError())
    return false;
  if (!document->IsObject())
    return false;
  if (!document->HasMember("id") || !(*document)["id"].IsInt())
    return false;
  *result = (*document)["id"].GetInt();
  return true;
}

// this is json wrapped api, it's raw avro json wrapped in a string
bool decode_get_schema_request_response(char* buf, size_t len, std::shared_ptr<avro::ValidSchema> schema) {
  try {
    auto document = std::make_shared<rapidjson::Document>();
    std::string avro_schema;
    document->Parse(buf, len);
    if (document->HasParseError())
      return false;
    if (!document->IsObject())
      return false;
    if (!document->HasMember("schema") || !(*document)["schema"].IsString())
      return false;
    avro_schema = (*document)["schema"].GetString();
    //std::cerr << avro_schema << std::endl;
    avro::compileJsonSchema(std::istringstream(avro_schema), *schema);
  }
  catch (...) {
 // failed to parse this...
    schema.reset();
    return false;
  }
  return true;
}

registry::registry(boost::asio::io_service& ios, std::vector<std::string> base_urls) :
  _http(ios),
  _base_urls(base_urls) {}

void registry::put_schema(std::string schema_name, std::shared_ptr<avro::ValidSchema> schema, put_callback put_cb) {
  auto shared_result = std::make_shared<rpc_put_schema_result>();
  auto work = std::make_shared<csi::async::work<int>>(csi::async::SEQUENTIAL, csi::async::FIRST_SUCCESS); // should we do random order??  can we send rpc result to work...
  auto encoded_string = encode_put_schema_request(schema);
  //std::cerr << encoded_string << std::endl;

  for (auto&& i : _base_urls) {
    std::string uri = i + "/subjects/" + schema_name + "/versions";
    work->push_back([this, uri, encoded_string, shared_result](csi::async::work<int>::callback cb) {
      std::vector<std::string> headers = {"Content-Type: application/vnd.schemaregistry.v1+json"};
      auto request = std::make_shared<csi::http::request>(csi::http::POST, uri, headers, 100ms);
      request->append(encoded_string);
      _http.perform_async(request, [cb, shared_result](std::shared_ptr<csi::http::request> request) {
        if (request->http_result() >= 200 && request->http_result() < 300) {
          BOOST_LOG_TRIVIAL(trace) << "confluent::registry::put_schema on_complete data: " << request->uri() << " got " << request->rx_content_length() << " bytes";
          std::string copy_of_bytes(request->rx_content()); // TBD remove me!!!
          if (decode_put_schema_request_response((char*) request->rx_content(), request->rx_content_length(), &shared_result->schema_id)) { // the json parser overwrites the internal buffer so logging before this line
            BOOST_LOG_TRIVIAL(trace) << "confluent::registry::put_schema returned id:" << shared_result->schema_id;
            cb(0);
            return;
          } else {
            BOOST_LOG_TRIVIAL(error) << "confluent::registry::put_schema return value unexpected bytes:" << copy_of_bytes;
          }
        } else {
          BOOST_LOG_TRIVIAL(error) << "confluent::registry::put_schema: " << request->uri() << " HTTPRES = " << request->http_result() << ", " << (request->rx_content_length() ? request->rx_content() : "");
        }
        cb(-1);
      });
    });
  }
  work->async_call([work, shared_result, put_cb](int64_t duration, int ec) {
    shared_result->ec = ec;
    put_cb(*shared_result);
  });
}

void registry::get_schema(int32_t schema_id, get_callback get_cb) {
  auto shared_result = std::make_shared<rpc_get_result>();
  auto work = std::make_shared<csi::async::work<int>>(csi::async::SEQUENTIAL, csi::async::FIRST_SUCCESS); // should we do random order??  can we send rpc result to work...
  for (auto&& i : _base_urls) {
    std::string uri = i + "/schemas/ids/" + std::to_string(schema_id);
    work->push_back([this, uri, shared_result](csi::async::work<int>::callback cb) {
      std::vector<std::string> headers = {"Accept: application/vnd.schemaregistry.v1+json"};
      auto request = std::make_shared<csi::http::request>(csi::http::GET, uri, headers, 100ms); // move to api
      _http.perform_async(request, [cb, shared_result](std::shared_ptr<csi::http::request> request) {
        if (request->http_result() >= 200 && request->http_result() < 300) {
          BOOST_LOG_TRIVIAL(trace) << "confluent::registry::get_schema on_complete data: " << request->uri() << " got " << request->rx_content_length() << " bytes";
          std::string copy_of_bytes(request->rx_content());
          shared_result->schema = std::make_shared<avro::ValidSchema>();
          if (decode_get_schema_request_response((char*) request->rx_content(), request->rx_content_length(), shared_result->schema)) { // the json parser overwrites the internal buffer so logging before this line
            //BOOST_LOG_TRIVIAL(trace) << "confluent::registry::get_schema returned id:" << shared_result->result;
            cb(0);
            return;
          } else {
            BOOST_LOG_TRIVIAL(error) << "confluent::registry::get_schema return value unexpected bytes:" << copy_of_bytes;
          }
        } else {
          BOOST_LOG_TRIVIAL(error) << "confluent::registry::get_schema: " << request->uri() << " HTTPRES = " << request->http_result() << ", " << (request->rx_content_length() ? request->rx_content() : "");
        }
        cb(-1);
      });
    });
  }
  work->async_call([work, shared_result, get_cb](int64_t duration, int ec) {
    shared_result->ec = ec;
    get_cb(*shared_result);
  });
}

//void registry::get_schema_by_id(int32_t id, get_callback cb) {
//  //we should check that we don't have it in cache and post reply directy in trhat case. TBD

//  std::string uri = "http://" + _address + "/schemas/ids/" + std::to_string(id);

//  _http.perform_async(
//    csi::create_http_request(csi::http::GET, uri, { "Content-Type: application/vnd.schemaregistry.v1+json" },
//    std::chrono::milliseconds(1000)),
//    [this, id, cb](csi::http_client::call_context::handle state) {
//    if(state->http_result() >= 200 && state->http_result() < 300) {
//      BOOST_LOG_TRIVIAL(debug) << "confluent::registry get_schema_by_id: " << state->uri() << " -> " << to_string(state->rx_content());
//      get_schema_by_id_resp resp;
//      if(csi::json_spirit_decode(state->rx_content(), resp)) {
//        //mutex??
//        std::map<int32_t, boost::shared_ptr<avro::ValidSchema>>::const_iterator item = _registry.find(id);
//        if(item != _registry.end()) {
//          cb(state, item->second); // return the one we already have - kill the new one...
//          return;
//        }
//        std::pair<int32_t, boost::shared_ptr<avro::ValidSchema>> x(id, resp._schema);
//        _registry.insert(x);
//        cb(state, resp._schema);
//        return;
//      } else {
//        BOOST_LOG_TRIVIAL(error) << "confluent::registry::get_schema_by_id return value unexpected: " << to_string(state->rx_content());
//        cb(state, NULL); // fix a bad state here!!!
//        return;
//      }
//    } else {
//      BOOST_LOG_TRIVIAL(error) << "confluent::registry get_schema_by_id uri: " << state->uri() << " HTTPRES = " << state->http_result();
//    }
//    cb(state, NULL);
//  });
//}

//std::shared_ptr<avro::ValidSchema> registry::get_schema_by_id(int32_t id) {
//  //lookup in cache first
//  //mutex???
//  std::map<int32_t, boost::shared_ptr<avro::ValidSchema>>::const_iterator item = _registry.find(id);
//  if(item != _registry.end())
//    return item->second;

//  //std::pair<std::shared_ptr<csi::http_client::call_context>, boost::shared_ptr<avro::ValidSchema>> 
//  std::promise<std::pair<std::shared_ptr<csi::http_client::call_context>, boost::shared_ptr<avro::ValidSchema>>> p;
//  std::future<std::pair<std::shared_ptr<csi::http_client::call_context>, boost::shared_ptr<avro::ValidSchema>>>  f = p.get_future();
//  get_schema_by_id(id, [&p](std::shared_ptr<csi::http_client::call_context> call_context, std::shared_ptr<avro::ValidSchema> schema) {
//    std::pair<std::shared_ptr<csi::http_client::call_context>, std::shared_ptr<avro::ValidSchema>> res(call_context, schema);
//    p.set_value(res);
//  });
//  f.wait();
//  std::pair<std::shared_ptr<csi::http_client::call_context>, std::shared_ptr<avro::ValidSchema>> res = f.get();
//  int32_t http_res = res.first->http_result();
//  if(http_res >= 200 && http_res < 300) {
//    //add to in cache first
//    return res.second;
//  }
//  //exception???
//  return NULL;
//}


//int32_t registry::get_cached_schema(boost::shared_ptr<avro::ValidSchema> p)
//{
// //lookup in cache first
// //mutex???
// std::map<int32_t, boost::shared_ptr<avro::ValidSchema>>::const_iterator item = _registry.find(id);
// if (item != _registry.end())
//	 return item->second;

//}

}; // namespace