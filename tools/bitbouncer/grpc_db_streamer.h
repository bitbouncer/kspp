#include <string>
#include <kspp/kspp.h>
#include <kspp/topology_builder.h>
#include <kspp/processors/visitor.h>
#include <kspp/connect/bitbouncer/grpc_streaming_source.h>
#pragma once

template<class K, class V>
class grpc_db_streamer{
public:
  typedef std::function<void(const kspp::krecord <K, V>& record)> extractor;
  grpc_db_streamer(std::shared_ptr<kspp::cluster_config> config, std::string offset_storage, std::string uri, std::string api_key, std::string secret_access_key, std::string topic, extractor f)
      : _config(config)
      ,_offset_storage(offset_storage)
      , _builder(config)
      , _uri(uri)
      , _api_key(api_key)
      , _secret_access_key(secret_access_key)
      , _topic(topic)
      , _f(f)
      , _connected(false)
      , _next_log (0)
      , _next_commit(kspp::milliseconds_since_epoch() + 10000){
    connect();
  }

  int64_t process(){
    if (!_main->good()) {
      // time to reconnecdt - wait 5-10-20,30,60s
      connect();
      return 0;
    }
    auto sz = _main->process(kspp::milliseconds_since_epoch());
    if (kspp::milliseconds_since_epoch() > _next_commit) {
      _next_commit = kspp::milliseconds_since_epoch() + 10000;
      _main->commit(false);
    }

    return sz;
  }

private:
  void connect(){
    LOG(INFO) << "reconnecting";
    grpc::ChannelArguments channelArgs;
    channelArgs.SetInt(GRPC_ARG_KEEPALIVE_TIME_MS, 10000);
    channelArgs.SetInt(GRPC_ARG_KEEPALIVE_TIMEOUT_MS, 10000);
    channelArgs.SetInt(GRPC_ARG_HTTP2_MIN_SENT_PING_INTERVAL_WITHOUT_DATA_MS, 10000);
    channelArgs.SetInt(GRPC_ARG_HTTP2_MAX_PINGS_WITHOUT_DATA, 0);
    auto channel_creds = grpc::SslCredentials(grpc::SslCredentialsOptions());
    _channel = grpc::CreateCustomChannel(_uri, channel_creds, channelArgs);
    _main = _builder.create_topology();
    _main_source = _main->create_processor<kspp::grpc_streaming_source<K, V>>(0, _topic, _offset_storage, _channel, _api_key, _secret_access_key);
    _main->create_processor<kspp::visitor<K, V>>(_main_source, [&](const auto &in) {
      _f(in);
    });
    _main->start(kspp::OFFSET_STORED);
    _connected = true;
  }

  std::shared_ptr<kspp::cluster_config> _config;
  std::string _offset_storage;
  kspp::topology_builder _builder;
  std::string _uri;
  std::string _api_key;
  std::string _secret_access_key;
  std::string _topic;
  extractor _f;
  bool _connected;
  int64_t _next_log;
  int64_t _next_commit;
  std::shared_ptr<grpc::Channel> _channel;
  std::shared_ptr<kspp::topology> _main;
  std::shared_ptr<kspp::grpc_streaming_source<K, V>> _main_source;
};
