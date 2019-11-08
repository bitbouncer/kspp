#include <csignal>
#include <kspp/topology_builder.h>
#include <kspp/processors/visitor.h>
#include <kspp/connect/aws/kinesis_source.h>

#include <nlohmann/json.hpp>

using namespace std::chrono_literals;
using namespace kspp;
using json = nlohmann::json;

static bool run = true;
static void sigterm(int sig) {
  run = false;
}

int main(int argc, char **argv) {
  if (argc!=2){
    std::cerr << "usage: " << argv[0] << " stream_name";
    return -1;
  }

  FLAGS_logtostderr = 1;
  google::InitGoogleLogging(argv[0]);
  kspp::kinesis_consumer consumer(0, argv[1]);

  std::string stream_name = argv[1];


  /*auto nr_of_partitions = kspp::kafka::get_number_partitions(config, topic);
  if (partition_list.size() == 0 || partition_list[0] == -1)
    partition_list = kspp::get_partition_list(nr_of_partitions);
  LOG(INFO) << "partition_list   : " << kspp::partition_list_to_string(partition_list);
  */

  auto config = std::make_shared<kspp::cluster_config>("", 0);
  kspp::topology_builder generic_builder(config);

  auto t = generic_builder.create_topology();
  auto source0 = t->create_processors<kspp::kinesis_string_source>({0},stream_name);
  auto vistor = t->create_processors<kspp::visitor<std::string, std::string>>(source0, [](const auto record){
    if (record.value()) {
      json j = json::parse(*record.value());
      double t0 = j["ts"];
      auto now = kspp::milliseconds_since_epoch();
      int64_t kinesis_lag = now - record.event_time();
      int64_t total_lag = now -t0;
      LOG(INFO) << *record.value() << " kinesis ts: " << record.event_time() << ", kinesis lag: " << kinesis_lag << " total_lag: " << total_lag;
    }
    });

  std::signal(SIGINT, sigterm);
  std::signal(SIGTERM, sigterm);
  std::signal(SIGPIPE, SIG_IGN);

  LOG(INFO) << "status is up";

  t->start(kspp::OFFSET_END); // only this is implemneted in kinesis

  while (run) {
    if (t->process(kspp::milliseconds_since_epoch()) == 0) {
      std::this_thread::sleep_for(10ms);
      t->commit(false);
    }
  }

  LOG(INFO) << "status is down";

  return 0;
}


