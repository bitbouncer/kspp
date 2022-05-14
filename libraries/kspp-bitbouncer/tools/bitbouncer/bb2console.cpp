#include <iostream>
#include <csignal>
#include <boost/program_options.hpp>
#include <kspp/utils/env.h>
#include <kspp/utils/kafka_utils.h>
#include <kspp/processors/visitor.h>
#include <kspp/topology_builder.h>
#include <kspp/processors/visitor.h>
#include <kspp/utils/string_utils.h>
#include <kspp-bitbouncer/grpc_avro_source.h>

#define SERVICE_NAME     "bb_generic_avro_console_exporter"
#define DEFAULT_SRC_URI  "lb.bitbouncer.com:30112"
#define DEBUG_URI        "localhost:50063"

using namespace std::chrono_literals;
using namespace kspp;

static bool run = true;

static void sigterm(int sig) {
  run = false;
}

int main(int argc, char **argv) {
  FLAGS_logtostderr = 1;
  google::InitGoogleLogging(argv[0]);

  boost::program_options::options_description desc("options");
  desc.add_options()
      ("help", "produce help message")
      ("monitor_uri",
       boost::program_options::value<std::string>()->default_value(get_env_and_log("MONITOR_URI", DEFAULT_SRC_URI)),
       "monitor_uri")
      ("monitor_api_key",
       boost::program_options::value<std::string>()->default_value(get_env_and_log_hidden("MONITOR_API_KEY", "")),
       "monitor_api_key")
      ("monitor_secret_access_key", boost::program_options::value<std::string>()->default_value(
          get_env_and_log_hidden("MONITOR_SECRET_ACCESS_KEY", "")), "monitor_secret_access_key")
      ("topic", boost::program_options::value<std::string>()->default_value("logs"), "topic")
      ("offset_storage",
       boost::program_options::value<std::string>()->default_value(get_env_and_log("OFFSET_STORAGE", "")),
       "offset_storage")
      ("start_offset", boost::program_options::value<std::string>()->default_value("OFFSET_END"), "start_offset")
      ("oneshot", "run to eof and exit");

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

  std::string monitor_uri;
  if (vm.count("monitor_uri")) {
    monitor_uri = vm["monitor_uri"].as<std::string>();
  } else {
    std::cerr << "--monitor_uri must specified" << std::endl;
    return -1;
  }

  std::string monitor_api_key;
  if (vm.count("monitor_api_key")) {
    monitor_api_key = vm["monitor_api_key"].as<std::string>();
  }

  if (monitor_api_key.size() == 0) {
    std::cerr << "--monitor_api_key must be defined" << std::endl;
    return -1;
  }

  std::string monitor_secret_access_key;
  if (vm.count("monitor_secret_access_key"))
    monitor_secret_access_key = vm["monitor_secret_access_key"].as<std::string>();

  std::string offset_storage;
  if (vm.count("offset_storage"))
    offset_storage = vm["offset_storage"].as<std::string>();

  if (offset_storage.empty())
    offset_storage = config->get_storage_root() + "/" + SERVICE_NAME + "-import-metrics.offset";

  kspp::start_offset_t start_offset = kspp::OFFSET_BEGINNING;
  try {
    if (vm.count("start_offset"))
      start_offset = kspp::to_offset(vm["start_offset"].as<std::string>());
  }
  catch (std::exception &e) {
    std::cerr << "start_offset must be one of OFFSET_BEGINNING / OFFSET_END / OFFSET_STORED";
    return -1;
  }

  std::string topic;
  if (vm.count("topic"))
    topic = vm["topic"].as<std::string>();

  bool oneshot = false;
  if (vm.count("oneshot"))
    oneshot = true;

  LOG(INFO) << "monitor_uri                 : " << monitor_uri;
  LOG(INFO) << "monitor_api_key             : " << monitor_api_key;
  LOG(INFO) << "monitor_secret_access_key   : " << monitor_secret_access_key;
  LOG(INFO) << "offset_storage              : " << offset_storage;
  LOG(INFO) << "topic                       : " << topic;
  LOG(INFO) << "start_offset                : " << kspp::to_string(start_offset);
  LOG(INFO) << "discovering facts...";
  if (oneshot)
    LOG(INFO) << "oneshot          : TRUE";


  std::shared_ptr<grpc::Channel> channel;
  grpc::ChannelArguments channelArgs;
  // special for easier debugging
  if (monitor_uri == DEBUG_URI) {
    channel = grpc::CreateCustomChannel(monitor_uri, grpc::InsecureChannelCredentials(), channelArgs);
  } else {
    auto channel_creds = grpc::SslCredentials(grpc::SslCredentialsOptions());
    channel = grpc::CreateCustomChannel(monitor_uri, channel_creds, channelArgs);
  }

  kspp::topology_builder generic_builder(config);

  auto live = generic_builder.create_topology();
  auto offset_provider = get_offset_provider(offset_storage);
  auto source = live->create_processor<kspp::grpc_avro_source<kspp::generic_avro, kspp::generic_avro>>(0, topic,
                                                                                                       offset_provider,
                                                                                                       channel,
                                                                                                       monitor_api_key,
                                                                                                       monitor_secret_access_key);
  live->create_processor<kspp::visitor<kspp::generic_avro, kspp::generic_avro>>(source, [](auto ev) {
    if (ev.value())
      std::cout << to_json(*ev.value()) << std::endl;
  });

  live->start(start_offset);

  std::signal(SIGINT, sigterm);
  std::signal(SIGTERM, sigterm);
  std::signal(SIGPIPE, SIG_IGN);

  while (run && source->good()) {
    auto sz = live->process(kspp::milliseconds_since_epoch());

    if (sz == 0) {
      std::this_thread::sleep_for(100ms);
      live->commit(false);
      continue;
    }

  }
  live->close();

  LOG(INFO) << "exiting";


  return 0;
}