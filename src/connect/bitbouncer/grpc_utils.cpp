#include <kspp/connect/bitbouncer/grpc_utils.h>

#define API_KEY_HEADER            "api-key"
#define SECRET_ACCESS_KEY_HEADER "secret-access-key"

namespace kspp {
  void add_api_key(grpc::ClientContext &client_context, const std::string &api_key) {
    client_context.AddMetadata(API_KEY_HEADER, api_key);
  }

  void add_api_key_secret(grpc::ClientContext &client_context, const std::string &api_key, const std::string &secret_access_key) {
    client_context.AddMetadata(API_KEY_HEADER, api_key);
    client_context.AddMetadata(SECRET_ACCESS_KEY_HEADER, secret_access_key);
  }
}





