#include <kspp/schema_registry/schema_registry_client.h>
#include <future>
#include <rapidjson/document.h>
#include <rapidjson/writer.h>
#include <rapidjson/stringbuffer.h>
#include <kspp/cluster_config.h>
#include <sstream>

namespace kspp {
schema_registry_client::schema_registry_client(const kspp::cluster_config &config)
      : work_(new boost::asio::io_service::work(ios_))
        , fail_fast_(config.get_fail_fast())
        , proxy_(std::make_shared<confluent_http_proxy>(ios_, config))
        , thread_([this] { ios_.run(); }) {
  }

  schema_registry_client::~schema_registry_client() {
    proxy_.reset();
    work_.reset();
    thread_.join();
  }

  bool schema_registry_client::validate() {
    std::chrono::steady_clock::time_point begin = std::chrono::steady_clock::now();
    auto future = proxy_->get_config();
    future.wait();
    auto rpc_result = future.get();
    if (rpc_result.ec) {
      LOG(WARNING) << "schema_registry_client validate failed: ec" << rpc_result.ec;
      return false;
    }
    std::chrono::steady_clock::time_point end = std::chrono::steady_clock::now();
    LOG(INFO) << "schema_registry_client validate OK "
              << std::chrono::duration_cast<std::chrono::milliseconds>(end - begin).count() << " ms";
    return true;
  }

  int32_t schema_registry_client::put_schema(std::string name, std::shared_ptr<const avro::ValidSchema> schema) {
    auto future = proxy_->put_schema(name, schema);
    future.wait();
    auto rpc_result = future.get();
    if (rpc_result.ec) {
      LOG_IF(FATAL, fail_fast_) << "schema_registry_client put failed: ec" << rpc_result.ec;
      LOG(ERROR) << "schema_registry_client put failed: ec" << rpc_result.ec;
      return -1;
    }
    LOG(INFO) << "schema_registry_client put \"" << name << "\" -> " << rpc_result.schema_id;
    return rpc_result.schema_id;
  }

  int32_t schema_registry_client::put_schema(std::string subject, const nlohmann::json& schema) {
    auto future = proxy_->put_schema(subject, schema);
    future.wait();
    auto rpc_result = future.get();
    if (rpc_result.ec) {
      LOG(ERROR) << "schema_registry_client put failed: ec" << rpc_result.ec;
      LOG_IF(FATAL, fail_fast_) << "schema_registry_client put failed: ec" << rpc_result.ec;
      return -1;
    }
    LOG(INFO) << "schema_registry_client put \"" << subject << "\" -> " << rpc_result.schema_id;
    return rpc_result.schema_id;
  }

  std::shared_ptr<const avro::ValidSchema> schema_registry_client::get_avro_schema(int32_t schema_id) {
    {
      kspp::spinlock::scoped_lock xxx(spinlock_);
      {
        std::map<int32_t, std::shared_ptr<const avro::ValidSchema>>::iterator item = avro_cache_.find(schema_id);
        if (item != avro_cache_.end()) {
          //DLOG_EVERY_N(INFO, 1000) << "schema_registry_client, cache lookup on " << schema_id;
          return item->second;
        }
      }
    }

    auto future = proxy_->get_avro_schema(schema_id);
    future.wait();
    auto rpc_result = future.get();
    if (rpc_result.ec) {
      //LOG_IF(FATAL, _fail_fast) << "avro_schema_registry get failed: ec" << rpc_result.ec;
      LOG(ERROR) << "schema_registry_client get failed: ec" << rpc_result.ec;
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
    LOG(INFO) << "schema_registry_client get " << schema_id << "-> " << buffer.GetString();

    kspp::spinlock::scoped_lock xxx(spinlock_);
    {
      avro_cache_[schema_id] = rpc_result.schema;
    }
    return rpc_result.schema;
  }


  std::shared_ptr<const nlohmann::json> schema_registry_client::get_json_schema(int32_t schema_id) {
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
      LOG(ERROR) << "schema_registry_client get failed: ec" << rpc_result.ec;
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


  std::shared_ptr<const nlohmann::json> schema_registry_client::get_json_schema(std::string subject){
    /*{
  kspp::spinlock::scoped_lock xxx(spinlock_);
  {
    std::map<int32_t, std::shared_ptr<const avro::ValidSchema>>::iterator item = cache_.find(schema_id);
    if (item != cache_.end()) {
      //DLOG_EVERY_N(INFO, 1000) << "schema_registry_client, cache lookup on " << schema_id;
      return item->second;
    }
  }
}
*/

    // we need to handle subjects like google/protobuf/timestamp.proto that should be translated to
    // google%2fprotobuf%2ftimestamp.proto
    auto future = proxy_->get_json_schema(subject);
    future.wait();
    auto rpc_result = future.get();
    if (rpc_result.ec == 404) {
      LOG(ERROR) << "schema_registry_client get_json_schema, subject: " << subject
                 << " not found";
      return nullptr;
    }

    if (rpc_result.ec){
      LOG(ERROR) << "schema_registry_client get_json_schema, subject: " << subject
                 << " failed, ec: " << rpc_result.ec;
      return nullptr;
    }

    std::stringstream ss;
    ss << rpc_result.schema;
    //DLOG(INFO) << ss.str();
    //we hope that this is a json
    auto result = std::make_shared<nlohmann::json>(rpc_result.schema);

    kspp::spinlock::scoped_lock xxx(spinlock_);
    {
      //cache_[schema_id] = rpc_result.schema;
    }
    return std::make_shared<nlohmann::json>(rpc_result.schema);
  }


  nlohmann::json schema_registry_client::verify_schema(std::string subject, const nlohmann::json& schema){
    DLOG(INFO) << "verifying schema: "  << subject;
    //DLOG(INFO) << "pushing "  << schema.dump();
    // should throw exeption on error
    //int32_t id = put_schema(subject, schema);
    put_schema(subject, schema);
    // if we ask by id we get back a schema that does not contain
    // "schemaType":"PROTOBUF","subject":"other.proto"
    // if we ask by id we get back a complete schema
    auto returned_schema = get_json_schema(subject);
    if (!returned_schema)
      throw std::runtime_error("schema_registry_client::verify_schema failed to get" + subject);
    DLOG(INFO) << "verify schema: "  << subject << " OK";
    return *returned_schema;
  }

/*
  nlohmann::json protobuf_register_schema(std::shared_ptr<kspp::avro_schema_registry> registry, std::string subject, const google::protobuf::FileDescriptor* file_descriptor){
    nlohmann::json j;
    j["subject"] = subject;
    j["schemaType"] = "PROTOBUF";

    //std::string filename_name = file_descriptor->name();
    //std::string package_name = file_descriptor->package();

    int dep_sz = file_descriptor->dependency_count();
    if (dep_sz>0)
      j["references"] = nlohmann::json::array();

    // do we need to register sub schemas
    for (int i=0; i!= dep_sz; ++i){
      std::string dep_name = file_descriptor->dependency(i)->name();
      const google::protobuf::FileDescriptor* dep_fd  = file_descriptor->dependency(i);
      LOG(INFO) << dep_name;
      std::shared_ptr<const nlohmann::json> dep_schema = registry->get_json_schema(dep_name);
      if (dep_schema) {
        LOG(INFO) << "dep_name: " << dep_name << " -> " << dep_schema->dump();
        nlohmann::json dependency;
        dependency["name"]=dep_name;
        dependency["subject"]=dep_name;
        dependency["version"]= (*dep_schema)["version"];
        j["references"].push_back(dependency);
      } else {
        nlohmann::json dependency;
        auto json = protobuf_register_schema(registry, dep_name, dep_fd);
        LOG(INFO) << json.dump(2);
        dependency["version"]=json["version"];
        dependency["name"]=dep_name;
        dependency["subject"]=dep_name;
        j["references"].push_back(dependency);
      }
    }
    j["schema"] = file_descriptor->DebugString();;
    LOG(INFO) << "about to push subject: " << subject << "  " <<  j.dump(2);
    int32_t id = registry->put_schema(subject, j); // change to return json
    LOG(INFO) << "got back " << id;
    std::shared_ptr<const nlohmann::json> return_schema = registry->get_json_schema(subject);
    if (!return_schema){
      LOG(ERROR) << "could not register schema";
    } else {
      j["version"] = (*return_schema)["version"];
      j["id"] = (*return_schema)["id"];
    }
    return j;
  }
}
*/

nlohmann::json protobuf_register_schema(std::shared_ptr<kspp::schema_registry_client> registry, std::string subject, const google::protobuf::FileDescriptor* file_descriptor){
  nlohmann::json j;
  j["subject"] = subject;
  j["schemaType"] = "PROTOBUF";
  j["schema"] = file_descriptor->DebugString();;

  int dep_sz = file_descriptor->dependency_count();
  if (dep_sz>0)
    j["references"] = nlohmann::json::array();
  for (int i=0; i!= dep_sz; ++i){
    std::string dep_name = file_descriptor->dependency(i)->name();
    const google::protobuf::FileDescriptor* dep_fd  = file_descriptor->dependency(i);
    auto dep_schema = protobuf_register_schema(registry, dep_name, dep_fd);
    nlohmann::json dependency;
    dependency["name"]=dep_name;
    dependency["subject"]=dep_name;
    dependency["version"]= dep_schema["version"];
    j["references"].push_back(dependency);
  }

  return registry->verify_schema(subject, j); // change to return json
}
}
