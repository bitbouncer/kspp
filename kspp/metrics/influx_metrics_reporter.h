#include <string>
#include <memory>
#include <thread>
#include <kspp/kspp.h>
#include <kspp/impl/serdes/text_serdes.h>
#include <kspp/sinks/kafka_sink.h>
#pragma once

namespace kspp {
class influx_metrics_reporter
{
  public:
  influx_metrics_reporter(kspp::topology_builder& builder, std::string topic, std::vector<std::shared_ptr<topic_topology>>, std::string prefix, std::string tags);
  ~influx_metrics_reporter();

  private:
  bool                                                                                 _run;
  const std::string                                                                    _topic;
  std::vector<std::shared_ptr<topic_topology>>                                         _reported_topologys;
  std::shared_ptr<topic_topology>                                                      _metrics_topology;
  std::shared_ptr<kspp::kafka_topic_sink<std::string, std::string, kspp::text_serdes>> _sink;
  const std::string                                                                    _prefix;
  const std::string                                                                    _tags;
  const std::string                                                                    _hostname;
  std::shared_ptr<std::thread>                                                         _thread;
};
};