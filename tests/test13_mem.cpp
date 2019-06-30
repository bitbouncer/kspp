#include <kspp/processors/generic_stream.h>  // processor/generic_stream -> /sources/mem_stream_source
#include <kspp/sinks/null_sink.h>
#include <kspp/kspp.h>
#include <kspp/topology_builder.h>


using namespace std::chrono_literals;


/* Exit flag for main loop */
static bool run = true;
static void sigterm(int sig) {
  run = false;
}

int main(int argc, char** argv){
  auto config = std::make_shared<kspp::cluster_config>("");
  kspp::topology_builder builder(config);
  auto topology = builder.create_topology();
  std::shared_ptr<kspp::generic_stream<std::string, std::string>> source = topology->create_processor<kspp::generic_stream<std::string, std::string>>(0);
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


  for(;;){
    for (int i=0; i!=100000; ++i)
      insert(*source, std::string("nisse"), std::string("was here"));
    std::this_thread::sleep_for(1000ms);
  }

  return 0;
}

