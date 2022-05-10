#include <iostream>
#include <csignal>
#include <boost/program_options.hpp>
#include <kspp/utils/env.h>
#include <kspp/utils/kafka_utils.h>
#include <kspp/topology_builder.h>
#include <kspp/sinks/avro_file_sink.h>
#include <kspp-bitbouncer/grpc_avro_source.h>

#define SERVICE_NAME     "bb2avro"
#define DEFAULT_SRC_URI  "lb.bitbouncer.com:30112"
#define DEBUG_URI        "localhost:50063"

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
      ("monitor_uri", boost::program_options::value<std::string>()->default_value(get_env_and_log("MONITOR_URI", DEFAULT_SRC_URI)), "monitor_uri")
      ("monitor_api_key", boost::program_options::value<std::string>()->default_value(get_env_and_log_hidden("MONITOR_API_KEY", "")), "monitor_api_key")
      ("monitor_secret_access_key", boost::program_options::value<std::string>()->default_value(get_env_and_log_hidden("MONITOR_SECRET_ACCESS_KEY", "")), "monitor_secret_access_key")
      ("topic", boost::program_options::value<std::string>()->default_value("logs"), "topic")
      ("offset_storage", boost::program_options::value<std::string>()->default_value(get_env_and_log("OFFSET_STORAGE", "")), "offset_storage")
      ("dst_dir", boost::program_options::value<std::string>()->default_value(get_env_and_log("DST_DIR", "/tmp")), "dst_dir")
      ("window_size_s", boost::program_options::value<std::string>()->default_value(get_env_and_log("WINDOW_SIZE_S", "3600")), "window_size_s")
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

  if (monitor_api_key.size()==0){
    std::cerr << "--monitor_api_key must be defined" << std::endl;
    return -1;
  }

  std::string monitor_secret_access_key;
  if (vm.count("monitor_secret_access_key"))
    monitor_secret_access_key = vm["monitor_secret_access_key"].as<std::string>();

  std::string topic;
  if (vm.count("topic")) {
    topic = vm["topic"].as<std::string>();
  }

  std::string dst_dir;
  if (vm.count("dst_dir")) {
    dst_dir = vm["dst_dir"].as<std::string>();
  }

  std::string offset_storage;
  if (vm.count("offset_storage"))
    offset_storage = vm["offset_storage"].as<std::string>();

  if (offset_storage.empty())
    offset_storage = config->get_storage_root() + "/" + SERVICE_NAME + "-import-metrics.offset";

  int window_size_s;
  if (vm.count("window_size_s")) {
    auto s = vm["window_size_s"].as<std::string>();
    window_size_s = atoi(s.c_str());
  }

  if (window_size_s<=0) {
    std::cerr << "window_size_s muist be >0";
    return -1;
  }


  bool oneshot=false;
  if (vm.count("oneshot"))
    oneshot=true;

  LOG(INFO) << "monitor_uri                 : " << monitor_uri;
  LOG(INFO) << "monitor_api_key             : " << monitor_api_key;
  LOG(INFO) << "monitor_secret_access_key   : " << monitor_secret_access_key;
  LOG(INFO) << "offset_storage              : " << offset_storage;
  LOG(INFO) << "topic                       : " << topic;
  LOG(INFO) << "dst_dir                     : " << dst_dir;
  LOG(INFO) << "window_size_s               : " << window_size_s << " s";
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
  auto t = generic_builder.create_topology();
  auto offset_provider = get_offset_provider(offset_storage);
  auto source = t->create_processor<kspp::grpc_avro_source<void, kspp::generic_avro>>(0, topic, offset_provider, channel, monitor_api_key, monitor_secret_access_key);
  auto sink = t->create_sink<kspp::avro_file_sink<kspp::generic_avro>>(source, dst_dir, topic, 1h);


  t->add_labels( {
                     { "app_name", SERVICE_NAME },
                     { "hostname", default_hostname() }
                 });

  t->start(kspp::OFFSET_STORED);

  std::signal(SIGINT, sigterm);
  std::signal(SIGTERM, sigterm);
  std::signal(SIGPIPE, SIG_IGN);

  int64_t next_commit = kspp::milliseconds_since_epoch() + 10000;

  while (run && source->good()) {
    auto sz = t->process(kspp::milliseconds_since_epoch());

    if (kspp::milliseconds_since_epoch()>next_commit){
      t->commit(false);
      next_commit = kspp::milliseconds_since_epoch() + 10000;
    }

    if (oneshot && t->eof()){
      LOG(INFO) << "at eof - flushing";
      t->flush(true);
      LOG(INFO) << "at eof - exiting";
      break;
    }

    if (sz == 0) {
      std::this_thread::sleep_for(100ms);
      continue;
    }
  }

  LOG(INFO) << "exiting";


  return 0;
}
