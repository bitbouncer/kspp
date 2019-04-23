#include <iostream>
#include <csignal>
#include <boost/program_options.hpp>
#include <kspp/utils/env.h>
#include <kspp/utils/kafka_utils.h>
#include <kspp/processors/visitor.h>
#include <kspp/topology_builder.h>
#include <kspp/processors/visitor.h>
#include <kspp/connect/bitbouncer/grpc_streaming_source.h>

#define SERVICE_NAME     "bb2pg"
#define DEFAULT_SRC_URI  "lb.bitbouncer.com:10063"

using namespace std::chrono_literals;
using namespace kspp;

static bool run = true;
static void sigterm(int sig) {
  run = false;
}

int main(int argc, char** argv) {
  FLAGS_logtostderr = 1;
  google::InitGoogleLogging(argv[0]);

  boost::program_options::options_description desc("options");
  desc.add_options()
      ("help", "produce help message")
      ("src_uri", boost::program_options::value<std::string>()->default_value(get_env_and_log("SRC_URI", DEFAULT_SRC_URI)), "src_uri")
      ("api_key", boost::program_options::value<std::string>()->default_value(get_env_and_log_hidden("API_KEY", "")), "api_key")
      ("api_secret", boost::program_options::value<std::string>()->default_value(get_env_and_log_hidden("API_SECRET", "")), "api_secret")
      ("topic", boost::program_options::value<std::string>()->default_value("logs"), "topic")
      ("partition_list", boost::program_options::value<std::string>()->default_value("[-1]"), "partition_list")
      ("offset_storage", boost::program_options::value<std::string>(), "offset_storage")
      ("oneshot", "run to eof and exit")
      ;

  boost::program_options::variables_map vm;
  boost::program_options::store(boost::program_options::parse_command_line(argc, argv, desc), vm);
  boost::program_options::notify(vm);

  if (vm.count("help")) {
    std::cout << desc << std::endl;
    return 0;
  }

  std::string consumer_group(SERVICE_NAME);
  auto config = std::make_shared<kspp::cluster_config>(consumer_group, kspp::cluster_config::NONE);
  config->load_config_from_env();

  std::string src_uri;
  if (vm.count("src_uri")) {
    src_uri = vm["src_uri"].as<std::string>();
  } else {
    std::cerr << "--src_uri must specified" << std::endl;
    return -1;
  }

  std::string api_key;
  if (vm.count("api_key")) {
    api_key = vm["api_key"].as<std::string>();
  } else {
    std::cerr << "--api_key must specified" << std::endl;
    return -1;
  }

  std::string offset_storage;
  if (vm.count("offset_storage")) {
    offset_storage = vm["offset_storage"].as<std::string>();
  } else {
    offset_storage = config->get_storage_root() + "/" + SERVICE_NAME + "-import-metrics.offset";
  }

  std::string topic;
  if (vm.count("topic")) {
    topic = vm["topic"].as<std::string>();
  }

  std::vector<int> partition_list;
  if (vm.count("partition_list")) {
    auto s = vm["partition_list"].as<std::string>();
    partition_list = kspp::parse_partition_list(s);
  }

  bool oneshot=false;
  if (vm.count("oneshot"))
    oneshot=true;

  LOG(INFO) << "src_uri          : " << src_uri;
  LOG(INFO) << "api_key          : [hidden]";
  LOG(INFO) << "offset_storage   : " << offset_storage;
  LOG(INFO) << "topic            : " << topic;
  LOG(INFO) << "discovering facts...";
  if (oneshot)
    LOG(INFO) << "oneshot          : TRUE";

  kspp::topology_builder generic_builder(config);
  grpc::ChannelArguments channelArgs;
  channelArgs.SetInt(GRPC_ARG_KEEPALIVE_TIME_MS, 10000);
  channelArgs.SetInt(GRPC_ARG_KEEPALIVE_TIMEOUT_MS, 10000);
  channelArgs.SetInt(GRPC_ARG_KEEPALIVE_PERMIT_WITHOUT_CALLS, 1);
  channelArgs.SetInt(GRPC_ARG_HTTP2_MIN_SENT_PING_INTERVAL_WITHOUT_DATA_MS, 10000);
  channelArgs.SetInt(GRPC_ARG_HTTP2_MAX_PINGS_WITHOUT_DATA, 0);
  auto channel_creds = grpc::SslCredentials(grpc::SslCredentialsOptions());
  auto channel = grpc::CreateCustomChannel(src_uri, channel_creds, channelArgs);

  auto live = generic_builder.create_topology();
  auto source = live->create_processor<kspp::grpc_streaming_source<kspp::generic_avro, kspp::generic_avro>>(0, topic, offset_storage, channel, api_key);
  live->create_processor<kspp::visitor<kspp::generic_avro,kspp::generic_avro>>(source, [](auto ev){
    if (ev.value())
      std::cout << to_json(*ev.value()) << std::endl;
  });

  live->start(kspp::OFFSET_END);

  std::signal(SIGINT, sigterm);
  std::signal(SIGTERM, sigterm);
  std::signal(SIGPIPE, SIG_IGN);

  while (run && source->good()) {
    auto sz = live->process(kspp::milliseconds_since_epoch());

    if (sz == 0) {
      std::this_thread::sleep_for(100ms);
      continue;
    }

    }

  LOG(INFO) << "exiting";


  return 0;
}