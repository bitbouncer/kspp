#include <grpcpp/grpcpp.h>
#pragma once

namespace kspp {
  void add_api_key(grpc::ClientContext &client_context, const std::string &api_key);
  void add_api_key_secret(grpc::ClientContext &client_context, const std::string &api_key, const std::string &secret);
}



