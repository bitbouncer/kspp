#include <kspp/metrics/prometheus_pushgateway_reporter.h>
#include <kspp/topology_builder.h>
using namespace std::chrono_literals;
namespace kspp {
  static std::string hostname_part(std::string s){
    return s.substr(0, s.find(":"));
  }

  static std::string port_part(std::string s){
    auto i = s.find(":");
    if (i==std::string::npos)
      return "9091";
    return s.substr(i+1);
  }

  prometheus_pushgateway_reporter::prometheus_pushgateway_reporter(std::string job_name, std::string uri, bool verbose)
          : _run(true)
          , _gateway(hostname_part(uri), port_part(uri), job_name)
          , _verbose(verbose) {
        _thread = std::make_shared<std::thread>([this]() {

       int64_t next_time_to_send = kspp::milliseconds_since_epoch() + 10 * 1000;

      while (_run) {
        //time for report

        if (next_time_to_send <= kspp::milliseconds_since_epoch()) {
          uint64_t measurement_time = milliseconds_since_epoch();
          int http_result = _gateway.Push();
          uint64_t push_time = milliseconds_since_epoch();
          if (http_result!=200){
            LOG(WARNING) << "metrics push failed, elapsed: " << push_time - measurement_time;
          } else {
            if (_verbose)
              LOG(INFO) << "metrics sent OK, elapsed: " << push_time - measurement_time;
          }
          //schedule nex reporting event
          next_time_to_send += 10000;
          // if we are really out of sync lets sleep at least 10 more seconds
          if (next_time_to_send <= kspp::milliseconds_since_epoch())
            next_time_to_send = kspp::milliseconds_since_epoch() + 10000;
        }
        std::this_thread::sleep_for(100ms);
      } // while
    });//thread
  }

  prometheus_pushgateway_reporter::~prometheus_pushgateway_reporter() {
    _run = false;
    _thread->join();
  }

void prometheus_pushgateway_reporter::add_metrics(std::shared_ptr<topology> p) {
  _gateway.RegisterCollectable(p->get_prometheus_registry());
}

  std::shared_ptr<prometheus_pushgateway_reporter>
  operator<<(std::shared_ptr<prometheus_pushgateway_reporter> reporter, std::shared_ptr<topology> t) {
    reporter->add_metrics(t);
   return reporter;
}

  std::shared_ptr<prometheus_pushgateway_reporter>
  operator<<(std::shared_ptr<prometheus_pushgateway_reporter> reporter, std::vector<std::shared_ptr<topology>> v) {
    for (const auto& i : v)
      reporter->add_metrics(i);
    return reporter;
  }
}
