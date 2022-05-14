#include <gflags/gflags.h>
#include <kspp/sources/mem_stream_source.h>
#include <kspp/sinks/null_sink.h>
#include <kspp/kspp.h>
#include <kspp/topology_builder.h>

using namespace std::chrono_literals;


/* Exit flag for main loop */
static bool run = true;

static void sigterm(int sig) {
  run = false;
}

//valgrind  --leak-check=yes --leak-check=full --show-leak-kinds=all

int main(int argc, char **argv) {
  auto config = std::make_shared<kspp::cluster_config>("", kspp::cluster_config::NONE);
  kspp::topology_builder builder(config);
  auto topology = builder.create_topology();
  auto source = topology->create_processor<kspp::mem_stream_source<std::string, std::string>>(0);
  auto sink = topology->create_sink<kspp::null_sink<std::string, std::string>>(source);

  std::signal(SIGINT, sigterm);
  std::signal(SIGTERM, sigterm);
  std::signal(SIGPIPE, SIG_IGN);

  topology->start(kspp::OFFSET_END); // has to be something - since we feed events from web totally irrelevant

  std::thread t([topology]() {
    while (run) {
      if (topology->process(kspp::milliseconds_since_epoch()) == 0) {
        std::this_thread::sleep_for(10ms);
        topology->commit(false);
      }
    }
    LOG(INFO) << "flushing events..";
    topology->flush(true, 10000); // 10sec max
    LOG(INFO) << "flushing events done";
  });


  for (int i = 0; i != 10; ++i) {
    for (int j = 0; j != 1000; ++j)
      insert(*source, std::string("nisse"), std::string("was here"));
    std::this_thread::sleep_for(1000ms);
  }
  LOG(INFO) << "exiting test";

  run = false;
  t.join();
  LOG(INFO) << "joined";


  config->load_config_from_env();
  config->set_producer_buffering_time(1000ms);
  config->set_consumer_buffering_time(500ms);
  config->validate();
  config->log();
  config->get_schema_registry();


  gflags::ShutDownCommandLineFlags();
  return 0;
}

