#include <kspp/avro/schema_registry.h>
#include <rapidjson/document.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/prettywriter.h>
#include <avro/Compiler.hh>
#include <kspp/utils/async.h>
#include <kspp/utils/cluster_uri.h>

using namespace std::chrono_literals;
namespace kspp {
  static inline std::string normalize(const avro::ValidSchema &vs) {
    std::stringstream ss;
    vs.toJson(ss);
    std::string s = ss.str();
    // TBD we should strip type : string to string
    // strip whitespace
    s.erase(remove_if(s.begin(), s.end(), ::isspace), s.end());  // c version does not use locale...
    return s;
  }

  static inline void add_member(std::shared_ptr<rapidjson::Document> document, std::string key, std::string value) {
    rapidjson::Document::AllocatorType &allocator = document->GetAllocator();
    rapidjson::Value ks;
    rapidjson::Value vs;
    ks.SetString(key.c_str(), allocator);
    vs.SetString(value.c_str(), allocator);
    document->AddMember(ks, vs, allocator);
  }

  std::string encode_put_schema_request(std::shared_ptr<const avro::ValidSchema> schema) {
    auto s = normalize(*schema);
    auto document = std::make_shared<rapidjson::Document>();
    document->SetObject();
    add_member(document, "schema", s);
    rapidjson::StringBuffer buffer;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
    document->Accept(writer);
    return buffer.GetString();
  }

  bool decode_put_schema_request_response(char *buf, size_t len, int32_t *result) {
    auto document = std::make_shared<rapidjson::Document>();
    std::string tmp(buf, len);  // double copy - newer verison of rapidjson has Parse(char*, size_t len)
    document->Parse(tmp.c_str());
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
  bool decode_get_schema_request_response(char *buf, size_t len, std::shared_ptr<avro::ValidSchema> schema) {
    try {
      auto document = std::make_shared<rapidjson::Document>();
      std::string avro_schema;
      std::string tmp(buf, len);  // double copy - newer verison of rapidjson has Parse(char*, size_t len)
      document->Parse(tmp.c_str());

      if (document->HasParseError())
        return false;
      if (!document->IsObject())
        return false;
      if (!document->HasMember("schema") || !(*document)["schema"].IsString())
        return false;
      avro_schema = (*document)["schema"].GetString();
      //std::cerr << avro_schema << std::endl;
      std::istringstream stream(avro_schema);
      avro::compileJsonSchema(stream, *schema);
    }
    catch (...) {
      // failed to parse this...
      schema.reset();
      return false;
    }
    return true;
  }

  confluent_http_proxy::confluent_http_proxy(boost::asio::io_service &ios, std::shared_ptr<kspp::cluster_config> config)
      : _http(ios)
  , _http_timeout(config->get_schema_registry_timeout()){
    cluster_uri cu(config->get_schema_registry(), "http");
    LOG_IF(FATAL, !cu.good()) << "confluent_http_proxy bad uri: " << config->get_schema_registry();
    LOG_IF(FATAL, cu.authority().size() == 0) << "confluent_http_proxy bad uri: " << config->get_schema_registry();
    _base_urls = cu.split_authority();
  }

  void confluent_http_proxy::get_config(get_top_level_config_callback cb) {
    auto shared_result = std::make_shared<rpc_get_config_result>();
    auto work = std::make_shared<kspp::async::work<int>>(kspp::async::SEQUENTIAL,
                                                         kspp::async::FIRST_SUCCESS); // should we do random order??  can we send rpc result to work...
    for (auto &&i : _base_urls) {
      std::string uri = i + "/config";
      work->push_back([this, uri, shared_result](kspp::async::work<int>::callback cb) {
        std::vector<std::string> headers = {"Accept: application/vnd.schemaregistry.v1+json"};
        auto request = std::make_shared<kspp::http::request>(kspp::http::GET, uri, headers, _http_timeout); // move to api
        _http.perform_async(request, [cb, shared_result](std::shared_ptr<kspp::http::request> request) {
          if (request->http_result() >= 200 && request->http_result() < 300) {
            shared_result->config = request->rx_content();
            cb(0);
          }
          cb(-1);
        });
      });
    }
    work->async_call([work, shared_result, cb](int64_t duration, int ec) {
      shared_result->ec = ec;
      cb(*shared_result);
    });
  }

  void
  confluent_http_proxy::put_schema(std::string schema_name, std::shared_ptr<const avro::ValidSchema> schema,
                                   put_callback put_cb) {
    auto shared_result = std::make_shared<rpc_put_schema_result>();
    auto work = std::make_shared<kspp::async::work<int>>(kspp::async::SEQUENTIAL,
                                                         kspp::async::FIRST_SUCCESS); // should we do random order??  can we send rpc result to work...
    auto encoded_string = encode_put_schema_request(schema);
    //std::cerr << encoded_string << std::endl;

    for (auto &&i : _base_urls) {
      std::string uri = i + "/subjects/" + schema_name + "/versions";
      work->push_back([this, uri, encoded_string, shared_result](kspp::async::work<int>::callback cb) {
        std::vector<std::string> headers = {"Content-Type: application/vnd.schemaregistry.v1+json"};
        auto request = std::make_shared<kspp::http::request>(kspp::http::POST, uri, headers, 100ms);
        request->append(encoded_string);
        _http.perform_async(request, [cb, shared_result](std::shared_ptr<kspp::http::request> request) {
          if (request->http_result() >= 200 && request->http_result() < 300) {
#ifdef KSPP_DEBUG
            // the json parser overwrites the internal buffer so copy the response
            std::string copy_of_bytes(request->rx_content());
#endif
            if (decode_put_schema_request_response(
                (char *) request->rx_content(),
                request->rx_content_length(),
                &shared_result->schema_id)) {
              cb(0);
              return;
            }
#ifdef KSPP_DEBUG
              LOG(ERROR) << "confluent_http_proxy put_schema return value unexpected bytes:" << copy_of_bytes;
#else
              LOG(ERROR) << "confluent_http_proxy cannot parse response";
#endif
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

  void confluent_http_proxy::get_schema(int32_t schema_id, get_callback get_cb) {
    auto shared_result = std::make_shared<rpc_get_result>();
    auto work = std::make_shared<kspp::async::work<int>>(kspp::async::SEQUENTIAL,
                                                         kspp::async::FIRST_SUCCESS); // should we do random order??  can we send rpc result to work...
    for (auto &&i : _base_urls) {
      std::string uri = i + "/schemas/ids/" + std::to_string(schema_id);
      work->push_back([this, uri, shared_result](kspp::async::work<int>::callback cb) {
        std::vector<std::string> headers = {"Accept: application/vnd.schemaregistry.v1+json"};
        auto request = std::make_shared<kspp::http::request>(kspp::http::GET, uri, headers, 100ms); // move to api
        _http.perform_async(request, [cb, shared_result](std::shared_ptr<kspp::http::request> request) {
          if (request->http_result() >= 200 && request->http_result() < 300) {
            shared_result->schema = std::make_shared<avro::ValidSchema>();
#ifdef KSPP_DEBUG
            // the json parser overwrites the internal buffer so copy the response
            std::string copy_of_bytes(request->rx_content());
#endif
            if (decode_get_schema_request_response(
                (char *) request->rx_content(),
                request->rx_content_length(),
                shared_result->schema)) {
              cb(0);
              return;
            }
#ifdef KSPP_DEBUG
              LOG(ERROR) << "confluent_http_proxy get_schema return value unexpected bytes:" << copy_of_bytes;
#else
              LOG(ERROR) << "confluent_http_proxy cannot parse response";
#endif
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
} // kspp