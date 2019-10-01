#include <string>
#include <memory>
#include <thread>
#include <kspp/kspp.h>
#include <kspp/topology_builder.h>
#include <prometheus/gateway.h>
#pragma once

namespace kspp {
  class prometheus_pushgateway_reporter {
  public:
    prometheus_pushgateway_reporter(std::string job_name, std::string uri, bool verbose=false);

    ~prometheus_pushgateway_reporter();

    void add_metrics(std::shared_ptr<topology> p);

  private:
    bool _run;
    const std::string _uri;
    std::shared_ptr<std::thread> _thread;
    prometheus::Gateway _gateway;
    bool _verbose;
  };

  std::shared_ptr<prometheus_pushgateway_reporter>
  operator<<(std::shared_ptr<prometheus_pushgateway_reporter> reporter, std::shared_ptr<topology> t);

  std::shared_ptr<prometheus_pushgateway_reporter>
  operator<<(std::shared_ptr<prometheus_pushgateway_reporter> reporter, std::vector<std::shared_ptr<topology>> v);
}
