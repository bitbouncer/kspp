#include <avro/ValidSchema.hh>
#include <avro/Compiler.hh>
#include <grpcpp/grpcpp.h>
#include <kspp/utils/spinlock.h>
#include <kspp/connect/bitbouncer/grpc_utils.h>
#include <bitbouncer_streaming.grpc.pb.h>
#pragma once

namespace kspp {
  class grpc_avro_schema_resolver {
  public:
    grpc_avro_schema_resolver(std::shared_ptr<grpc::Channel> channel, const std::string &api_key)
        : stub_(ksppstreaming::streamprovider::NewStub(channel)), _api_key(api_key) {
    }

    std::shared_ptr<const avro::ValidSchema> get_schema(int32_t schema_id) {
      if (schema_id==0)
        return nullptr;

      {
        kspp::spinlock::scoped_lock xxx(_spinlock);
        auto item = _cache.find(schema_id);
        if (item != _cache.end()) {
          return item->second;
        }
      }

      grpc::ClientContext context;
      add_api_key(context, _api_key);
      ksppstreaming::GetSchemaRequest request;
      request.set_schema_id(schema_id);
      ksppstreaming::GetSchemaReply reply;
      grpc::Status status = stub_->GetSchema(&context, request, &reply);
      if (!status.ok()) {
        LOG(ERROR) << "avro_schema_resolver rpc failed.";
        return nullptr;
      }

      try {
        std::shared_ptr<avro::ValidSchema> schema = std::make_shared<avro::ValidSchema>();
        std::istringstream stream(reply.schema());
        avro::compileJsonSchema(stream, *schema);
        {
          kspp::spinlock::scoped_lock xxx(_spinlock);
          _cache[schema_id] = schema;
        }
        return schema;
      } catch (std::exception &e) {
        LOG(ERROR) << "failed to parse schema id:" << schema_id << ", " << e.what() << ", raw schema: " << reply.schema();
      }
      return nullptr;
    }

  private:
    kspp::spinlock _spinlock;
    std::unique_ptr<ksppstreaming::streamprovider::Stub> stub_;
    std::map<int32_t, std::shared_ptr<const avro::ValidSchema>> _cache;
    std::string _api_key;
  };
} // namespace
