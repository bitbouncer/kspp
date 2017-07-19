#include <kspp/topology_builder.h>
#include <kspp/impl/serdes/text_serdes.h>
#include <kspp/sinks/kafka_sink.h>
#include "influx_metrics_reporter.h"

using namespace std::chrono_literals;

namespace kspp {

  static std::string hostname() {
#ifdef _WIN32
    if (const char* env_p = std::getenv("COMPUTERNAME"))
      return std::string(env_p);
    else
      return "unknown";
#else
    char s[256];
    sprintf(s, "unknown");
    gethostname(s, 256);
    return std::string(s);
#endif
  }

  influx_metrics_reporter::influx_metrics_reporter(kspp::topology_builder &builder, std::string topic,
                                                   std::string prefix, std::string tags)
          : _run(true), _topic(topic), _prefix(prefix), _tags(tags), _hostname(hostname()) {
    _metrics_topology = builder.create_topology();
    _sink = _metrics_topology->create_sink<kspp::kafka_topic_sink<std::string, std::string, kspp::text_serdes>>(_topic);

    _thread = std::make_shared<std::thread>([this]() {
      std::string base_string = _prefix + (_tags.size()>0 ? "," + _tags : "");
      int64_t next_time_to_send = kspp::milliseconds_since_epoch() + 10 * 1000;

      while (_run) {
        while (_metrics_topology->process_one()) {
        }

        //time for report
        if (next_time_to_send <= kspp::milliseconds_since_epoch()) {
          uint64_t measurement_time = milliseconds_since_epoch();
          std::string measurement_time_str = std::to_string(milliseconds_since_epoch()) + "000000";

          for (auto i : _reported_t_topologys) {
            i->for_each_metrics([this, &base_string, &measurement_time_str, &measurement_time](kspp::metric &m) {
              std::string measurement_tags = m.tags();
              // influxdb line format
              std::string value =
                      base_string + (measurement_tags.size() ? "," + measurement_tags : "") + " " + m.name() + "=" +
                      std::to_string(m.value()) + "i " + measurement_time_str;
              _sink->produce(_hostname, value, measurement_time);
            });
          }

          //schedule nex reporting event
          next_time_to_send += 10000;
          // if we are reaaly out of sync lets sleep at least 10 more seconds
          if (next_time_to_send <= kspp::milliseconds_since_epoch())
            next_time_to_send = kspp::milliseconds_since_epoch() + 10000;
        }
        std::this_thread::sleep_for(500ms);
      } // while
    });//thread
  }

  influx_metrics_reporter::~influx_metrics_reporter() {
    _run = false;
    _thread->join();
  }

  void influx_metrics_reporter::add_metrics(std::shared_ptr<topology> p) {
    p->init_metrics();
    // maybe we need a mutex here... TBD
    _reported_t_topologys.push_back(p);
  }

  std::shared_ptr<influx_metrics_reporter>
  operator<<(std::shared_ptr<influx_metrics_reporter> reporter, std::shared_ptr<topology> t) {
    reporter->add_metrics(t);
    return reporter;
  }

  std::shared_ptr<influx_metrics_reporter>
  operator<<(std::shared_ptr<influx_metrics_reporter> reporter, std::vector<std::shared_ptr<topology>> v) {
    for (const auto& i : v)
      reporter->add_metrics(i);
    return reporter;
  }
};