#include <kspp/avro/avro_schema_registry.h>
#include <future>
#include <rapidjson/document.h>
#include <rapidjson/writer.h>
#include <rapidjson/stringbuffer.h>
#include <kspp/cluster_config.h>
#include <sstream>

namespace kspp {
  avro_schema_registry::avro_schema_registry(const kspp::cluster_config &config)
      : work_(new boost::asio::io_service::work(ios_))
        , fail_fast_(config.get_fail_fast())
        , proxy_(std::make_shared<confluent_http_proxy>(ios_, config))
        , thread_([this] { ios_.run(); }) {
  }

  avro_schema_registry::~avro_schema_registry() {
    proxy_.reset();
    work_.reset();
    thread_.join();
  }

  bool avro_schema_registry::validate() {
    std::chrono::steady_clock::time_point begin = std::chrono::steady_clock::now();
    auto future = proxy_->get_config();
    future.wait();
    auto rpc_result = future.get();
    if (rpc_result.ec) {
      LOG(WARNING) << "avro_schema_registry validate failed: ec" << rpc_result.ec;
      return false;
    }
    std::chrono::steady_clock::time_point end = std::chrono::steady_clock::now();
    LOG(INFO) << "avro_schema_registry validate OK "
              << std::chrono::duration_cast<std::chrono::milliseconds>(end - begin).count() << " ms";
    return true;
  }

  int32_t avro_schema_registry::put_schema(std::string name, std::shared_ptr<const avro::ValidSchema> schema) {
    auto future = proxy_->put_schema(name, schema);
    future.wait();
    auto rpc_result = future.get();
    if (rpc_result.ec) {
      LOG_IF(FATAL, fail_fast_) << "avro_schema_registry put failed: ec" << rpc_result.ec;
      LOG(ERROR) << "avro_schema_registry put failed: ec" << rpc_result.ec;
      return -1;
    }
    LOG(INFO) << "avro_schema_registry put \"" << name << "\" -> " << rpc_result.schema_id;
    return rpc_result.schema_id;
  }

  int32_t avro_schema_registry::put_schema(std::string name, const nlohmann::json& schema) {
    auto future = proxy_->put_schema(name, schema);
    future.wait();
    auto rpc_result = future.get();
    if (rpc_result.ec) {
      LOG_IF(FATAL, fail_fast_) << "avro_schema_registry put failed: ec" << rpc_result.ec;
      LOG(ERROR) << "avro_schema_registry put failed: ec" << rpc_result.ec;
      return -1;
    }
    LOG(INFO) << "avro_schema_registry put \"" << name << "\" -> " << rpc_result.schema_id;
    return rpc_result.schema_id;
  }

  std::shared_ptr<const avro::ValidSchema> avro_schema_registry::get_avro_schema(int32_t schema_id) {
    {
      kspp::spinlock::scoped_lock xxx(spinlock_);
      {
        std::map<int32_t, std::shared_ptr<const avro::ValidSchema>>::iterator item = cache_.find(schema_id);
        if (item != cache_.end()) {
          //DLOG_EVERY_N(INFO, 1000) << "avro_schema_registry, cache lookup on " << schema_id;
          return item->second;
        }
      }
    }

    auto future = proxy_->get_avro_schema(schema_id);
    future.wait();
    auto rpc_result = future.get();
    if (rpc_result.ec) {
      //LOG_IF(FATAL, _fail_fast) << "avro_schema_registry get failed: ec" << rpc_result.ec;
      LOG(ERROR) << "avro_schema_registry get failed: ec" << rpc_result.ec;
      return nullptr;
    }


    // multiline schema - bad for elastic search
    //std::stringstream ss;
    //rpc_result.schema->toJson(ss);
    //LOG(INFO) << "avro_schema_registry get " << schema_id << "-> " << ss.str();

    //replaced with
    // this is very ugly... todo find out another way
    std::stringstream ss;
    rpc_result.schema->toJson(ss);
    rapidjson::Document d;
    d.Parse(ss.str().c_str());
    rapidjson::StringBuffer buffer;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
    d.Accept(writer);
    LOG(INFO) << "avro_schema_registry get " << schema_id << "-> " << buffer.GetString();

    kspp::spinlock::scoped_lock xxx(spinlock_);
    {
      cache_[schema_id] = rpc_result.schema;
    }
    return rpc_result.schema;
  }


  std::shared_ptr<const nlohmann::json> avro_schema_registry::get_json_schema(int32_t schema_id) {
    /*{
      kspp::spinlock::scoped_lock xxx(spinlock_);
      {
        std::map<int32_t, std::shared_ptr<const avro::ValidSchema>>::iterator item = cache_.find(schema_id);
        if (item != cache_.end()) {
          //DLOG_EVERY_N(INFO, 1000) << "avro_schema_registry, cache lookup on " << schema_id;
          return item->second;
        }
      }
    }
    */

    auto future = proxy_->get_json_schema(schema_id);
    future.wait();
    auto rpc_result = future.get();
    if (rpc_result.ec) {
      //LOG_IF(FATAL, _fail_fast) << "avro_schema_registry get failed: ec" << rpc_result.ec;
      LOG(ERROR) << "avro_schema_registry get failed: ec" << rpc_result.ec;
      return nullptr;
    }

    // multiline schema - bad for elastic search
    //std::stringstream ss;
    //rpc_result.schema->toJson(ss);
    //LOG(INFO) << "avro_schema_registry get " << schema_id << "-> " << ss.str();

    //replaced with
    // this is very ugly... todo find out another way
    /*std::stringstream ss;
    rpc_result.schema->toJson(ss);
    rapidjson::Document d;
    d.Parse(ss.str().c_str());
    rapidjson::StringBuffer buffer;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
    d.Accept(writer);
    LOG(INFO) << "avro_schema_registry get " << schema_id << "-> " << buffer.GetString();
    */
    std::stringstream ss;
    ss << rpc_result.schema;
    LOG(INFO) << ss.str();

    kspp::spinlock::scoped_lock xxx(spinlock_);
    {
      //cache_[schema_id] = rpc_result.schema;
    }
    return std::make_shared<nlohmann::json>(rpc_result.schema);
  }
}
