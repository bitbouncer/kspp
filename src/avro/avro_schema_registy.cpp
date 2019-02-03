#include <kspp/avro/avro_schema_registry.h>
#include <kspp/cluster_config.h>
#include <future>

namespace kspp {
  avro_schema_registry::avro_schema_registry(const kspp::cluster_config& config)
      : _fail_fast(config.get_fail_fast())
      , _work(new boost::asio::io_service::work(_ios))
      , _thread([this] { _ios.run(); })
      , _proxy(std::make_shared<confluent_http_proxy>(_ios, config)) {
  }

  avro_schema_registry::~avro_schema_registry() {
    _proxy.reset();
    _work.reset();
    _thread.join();
  }

  bool avro_schema_registry::validate() {
    std::chrono::steady_clock::time_point begin = std::chrono::steady_clock::now();
    auto future = _proxy->get_config();
    future.wait();
    auto rpc_result = future.get();
    if (rpc_result.ec) {
      LOG(WARNING) << "avro_schema_registry validate failed: ec" << rpc_result.ec;
      return false;
    }
    std::chrono::steady_clock::time_point end= std::chrono::steady_clock::now();
    LOG(INFO) << "avro_schema_registry validate OK " << std::chrono::duration_cast<std::chrono::milliseconds> (end - begin).count() << " ms";
    return true;
  }

  int32_t avro_schema_registry::put_schema(std::string name, std::shared_ptr<const avro::ValidSchema> schema) {
    auto future = _proxy->put_schema(name, schema);
    future.wait();
    auto rpc_result = future.get();
    if (rpc_result.ec) {
      LOG_IF(FATAL, _fail_fast) << "avro_schema_registry put failed: ec" << rpc_result.ec;
      LOG(ERROR) << "avro_schema_registry put failed: ec" << rpc_result.ec;
      return -1;
    }
    LOG(INFO) << "avro_schema_registry put \"" << name << "\" -> " << rpc_result.schema_id;
    return rpc_result.schema_id;
  }

  std::shared_ptr<const avro::ValidSchema> avro_schema_registry::get_schema(int32_t schema_id) {
    {
      kspp::spinlock::scoped_lock xxx(_spinlock);
      {
        std::map<int32_t, std::shared_ptr<const avro::ValidSchema>>::iterator item = _cache.find(schema_id);
        if (item != _cache.end()) {
          //DLOG_EVERY_N(INFO, 1000) << "avro_schema_registry, cache lookup on " << schema_id;
          return item->second;
        }
      }
    }

    auto future = _proxy->get_schema(schema_id);
    future.wait();
    auto rpc_result = future.get();
    if (rpc_result.ec) {
      //LOG_IF(FATAL, _fail_fast) << "avro_schema_registry get failed: ec" << rpc_result.ec;
      LOG(ERROR) << "avro_schema_registry get failed: ec" << rpc_result.ec;
      return nullptr;
    }

    std::stringstream ss;
    rpc_result.schema->toJson(ss);
    LOG(INFO) << "avro_schema_registry get " << schema_id << "-> " << ss.str();

    kspp::spinlock::scoped_lock xxx(_spinlock);
    {
      _cache[schema_id] = rpc_result.schema;
    }
    return rpc_result.schema;
  }
}
