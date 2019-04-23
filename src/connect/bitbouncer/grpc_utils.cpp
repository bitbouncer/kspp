#include <kspp/connect/bitbouncer/grpc_utils.h>
namespace kspp {
  void add_api_key(grpc::ClientContext &client_context, const std::string &api_key) {
    client_context.AddMetadata("api-key", api_key);
  }

  void add_api_key_secret(grpc::ClientContext &client_context, const std::string &api_key, const std::string &secret) {
    client_context.AddMetadata("api-key", api_key);
    client_context.AddMetadata("api-secret", secret);
  }
}





