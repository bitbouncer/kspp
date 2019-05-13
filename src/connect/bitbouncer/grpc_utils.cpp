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

  void set_channel_args(grpc::ChannelArguments& channelArgs){
    //channelArgs.SetInt(GRPC_ARG_HTTP2_BDP_PROBE, 1);

    channelArgs.SetInt(GRPC_ARG_KEEPALIVE_TIME_MS, 1000);
    channelArgs.SetInt(GRPC_ARG_KEEPALIVE_TIMEOUT_MS, 1000);
    channelArgs.SetInt(GRPC_ARG_KEEPALIVE_PERMIT_WITHOUT_CALLS, 1);

    channelArgs.SetInt(GRPC_ARG_HTTP2_MAX_PINGS_WITHOUT_DATA, 0); // unlimited
    //channelArgs.SetInt(GRPC_ARG_HTTP2_MIN_RECV_PING_INTERVAL_WITHOUT_DATA_MS, 5000); // not applicable for client
    channelArgs.SetInt(GRPC_ARG_HTTP2_MIN_SENT_PING_INTERVAL_WITHOUT_DATA_MS, 1000);
  }
}





