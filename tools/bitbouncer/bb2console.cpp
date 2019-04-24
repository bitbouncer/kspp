#include <iostream>
#include <csignal>
#include <boost/program_options.hpp>
#include <kspp/utils/env.h>
#include <kspp/utils/kafka_utils.h>
#include <kspp/processors/visitor.h>
#include <kspp/topology_builder.h>
#include <kspp/processors/visitor.h>
#include <kspp/connect/bitbouncer/grpc_avro_source.h>

#define SERVICE_NAME     "bb_generic_avro_console_exporter"
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
      ("bb_api_key", boost::program_options::value<std::string>()->default_value(get_env_and_log_hidden("BB_API_KEY", "")), "bb_api_key")
      ("bb_secret_access_key", boost::program_options::value<std::string>()->default_value(get_env_and_log_hidden("BB_SECRET_ACCESS_KEY", "")), "bb_secret_access_key")
      ("topic", boost::program_options::value<std::string>()->default_value("logs"), "topic")
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

  std::string bb_api_key;
  if (vm.count("bb_api_key")) {
    bb_api_key = vm["bb_api_key"].as<std::string>();
  }

  if (bb_api_key.size()==0){
    std::cerr << "--bb_api_key must be defined" << std::endl;
    return -1;
  }

  std::string bb_secret_access_key;
  if (vm.count("bb_secret_access_key")) {
    bb_secret_access_key = vm["bb_secret_access_key"].as<std::string>();
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

  bool oneshot=false;
  if (vm.count("oneshot"))
    oneshot=true;

  LOG(INFO) << "src_uri                : " << src_uri;
  LOG(INFO) << "bb_api_key             : " << bb_api_key;
  if (bb_secret_access_key.size()>0)
    LOG(INFO) << "bb_secret_access_key   : " << "[hidden]";
  LOG(INFO) << "offset_storage         : " << offset_storage;
  LOG(INFO) << "topic                  : " << topic;
  LOG(INFO) << "discovering facts...";
  if (oneshot)
    LOG(INFO) << "oneshot          : TRUE";

  kspp::topology_builder generic_builder(config);

  auto live = generic_builder.create_topology();
  auto source = live->create_processor<kspp::grpc_avro_source<kspp::generic_avro, kspp::generic_avro>>(0, topic, offset_storage, src_uri, bb_api_key, bb_secret_access_key);
  live->create_processor<kspp::visitor<kspp::generic_avro,kspp::generic_avro>>(source, [](auto ev){
    if (ev.value())
      std::cout << to_json(*ev.value()) << std::endl;
  });

  live->start(kspp::OFFSET_BEGINNING);

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