#include <iostream>
#include <csignal>
#include <boost/program_options.hpp>
#include <kspp/utils/env.h>
#include <kspp/utils/kafka_utils.h>
#include <kspp/topology_builder.h>
#include <kspp/sources/kafka_source.h>
#include <kspp/sinks/avro_file_sink.h>

#ifdef KSPP_S3
#include <kspp/sinks/avro_s3_sink.h>
#endif

#include <kspp/utils/url.h>
#include <kspp/utils/string_utils.h>

#define SERVICE_NAME     "kafka2avro"

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
      ("partition_list", boost::program_options::value<std::string>()->default_value("[-1]"), "partition_list")
      ("start_offset", boost::program_options::value<std::string>()->default_value("OFFSET_STORED"), "start_offset")
      ("topic", boost::program_options::value<std::string>(), "topic")
      ("consumer_group", boost::program_options::value<std::string>()->default_value(get_env_and_log("CONSUMER_GROUP", "")), "consumer_group")
      ("dst", boost::program_options::value<std::string>()->default_value(get_env_and_log("DST", "")), "dst")
      ("window_size", boost::program_options::value<std::string>()->default_value(get_env_and_log("WINDOW_SIZE", "1h")), "window_size")
      ("oneshot", "run to eof and exit")
      ;

  boost::program_options::variables_map vm;
  boost::program_options::store(boost::program_options::parse_command_line(argc, argv, desc), vm);
  boost::program_options::notify(vm);

  if (vm.count("help")) {
    std::cout << desc << std::endl;
    return 0;
  }

  std::string consumer_group;
  if (vm.count("consumer_group")) {
    consumer_group = vm["consumer_group"].as<std::string>();
  }

  auto config = std::make_shared<kspp::cluster_config>(consumer_group);

  config->load_config_from_env();

  std::string topic;
  if (vm.count("topic")) {
    topic = vm["topic"].as<std::string>();
  }

  std::vector<int> partition_list;
  if (vm.count("partition_list")) {
    auto s = vm["partition_list"].as<std::string>();
    partition_list = kspp::parse_partition_list(s);
  }

  kspp::start_offset_t start_offset=kspp::OFFSET_BEGINNING;
  try {
    if (vm.count("start_offset"))
      start_offset = kspp::to_offset(vm["start_offset"].as<std::string>());
  }
  catch(std::exception& e) {
    std::cerr << "start_offset must be one of OFFSET_BEGINNING / OFFSET_END / OFFSET_STORED";
    return -1;
  }

  std::string dst_tmp;
  if (vm.count("dst")) {
    dst_tmp = vm["dst"].as<std::string>();
  }

  if (dst_tmp.empty()){
    std::cerr << "dst must be specified" << std::endl;
    return -1;
  }
  kspp::url dst_uri(dst_tmp, "file");

  std::string offset_storage;
  if (vm.count("offset_storage")) {
    offset_storage = vm["offset_storage"].as<std::string>();
  } else {
    offset_storage = config->get_storage_root() + "/" + SERVICE_NAME + "-" + topic + ".offset";
  }

  std::chrono::seconds window_size(0);
  if (vm.count("window_size")) {
    auto s = vm["window_size"].as<std::string>();
    window_size = to_duration(s);
  }

  if (window_size.count()<=0) {
    std::cerr << "window_size_s must be >0";
    return -1;
  }

  bool oneshot=false;
  if (vm.count("oneshot"))
    oneshot=true;



  LOG(INFO) << "topic                  : " << topic;
  LOG(INFO) << "start_offset           : " << kspp::to_string(start_offset);
  LOG(INFO) << "dst schema             : "  << dst_uri.scheme();
  LOG(INFO) << "dst authority          : "  << dst_uri.authority();
  LOG(INFO) << "dst path               : "  << dst_uri.path();
  LOG(INFO) << "window_size            : "  << to_string(window_size);

//  LOG(INFO) << "pushgateway_uri   : " << pushgateway_uri;
//  LOG(INFO) << "metrics_namespace : " << metrics_namespace;
  if (oneshot)
    LOG(INFO) << "oneshot          : TRUE";

  LOG(INFO) << "discovering facts...";

  auto nr_of_partitions = kspp::kafka::get_number_partitions(config, topic);
  if (partition_list.size() == 0 || partition_list[0] == -1)
    partition_list = kspp::get_partition_list(nr_of_partitions);
  LOG(INFO) << "partition_list   : " << kspp::partition_list_to_string(partition_list);

  kspp::topology_builder generic_builder(config);

  auto t = generic_builder.create_topology();
  auto source = t->create_processors<kspp::kafka_source<void, kspp::generic_avro, void, kspp::avro_serdes>>(partition_list, topic, config->avro_serdes());

  if (dst_uri.scheme()=="file")
    auto sink1 = t->create_sink<kspp::avro_file_sink<kspp::generic_avro>>(source, dst_uri.path(), topic, window_size);
#ifdef KSPP_S3
  if (dst_uri.scheme()=="s3")
    auto sink2 = t->create_sink<kspp::avro_s3_sink<kspp::generic_avro>>(source, dst_uri, topic, window_size);
#endif

  t->start(start_offset);

  std::signal(SIGINT, sigterm);
  std::signal(SIGTERM, sigterm);
  std::signal(SIGPIPE, SIG_IGN);
  LOG(INFO) << "status is up";

  int64_t next_commit = kspp::milliseconds_since_epoch() + 10000;

  while (run) {
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

  LOG(INFO) << "status is down";

  return 0;
}
