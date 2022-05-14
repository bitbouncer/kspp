#include <kspp/topology.h>
#include <kspp/kspp.h>
#include <kspp/utils/kafka_utils.h>
#include <algorithm>
#include <kspp/internal/commit_chain.h>

using namespace std::chrono_literals;

namespace kspp {
  topology::topology(std::shared_ptr<cluster_config> config, std::string topology_id, bool internal)
      : cluster_config_(config)
        , topology_id_(topology_id)
        , min_buffering_ms_(config->get_min_topology_buffering().count())
        , max_pending_sink_messages_(config->get_max_pending_sink_messages()) {
    if (internal)
      allow_commit_chain_gc_ = false;
    prom_registry_ = std::make_shared<prometheus::Registry>();
    LOG(INFO) << "topology created id:" << topology_id_;
  }

  topology::~topology() {
    LOG(INFO) << "topology terminating id:" << topology_id_;
    top_partition_processors_.clear();
    partition_processors_.clear();
    sinks_.clear();
    LOG(INFO) << "topology, terminating id:" << topology_id_;
  }

//the metrics should look like this...
//cpu_load_short, host=server01, region=us-west value=0.64 1434055562000000000
//metric_name,app_id={app_id}},topology={{_topology_id}},depth={{depth}},processor_type={{processor_name()}},record_type="
//order tags descending
  static std::string escape_influx(std::string s) {
    std::string s2 = boost::replace_all_copy(s, " ", "\\ ");
    std::string s3 = boost::replace_all_copy(s2, ",", "\\,");
    std::string s4 = boost::replace_all_copy(s3, "=", "\\=");
    return s4;
  }

  void topology::init_metrics() {
    for (auto &&i: partition_processors_) {
      for (auto j: labels_)
        i->add_metrics_label(j.first, j.second);

      i->add_metrics_label(KSPP_KEY_TYPE_TAG, escape_influx(i->key_type_name()));
      i->add_metrics_label(KSPP_VALUE_TYPE_TAG, escape_influx(i->value_type_name()));
      i->add_metrics_label(KSPP_PARTITION_TAG, std::to_string(i->partition()));

      //prometheus::Registry* xx = _prom_registry.get();

      for (auto &&j: i->get_metrics()) {
        j->finalize_labels(prom_registry_); // maybe add string escape function here...
      }
    }

    for (auto &&i: sinks_) {
      for (auto j: labels_)
        i->add_metrics_label(j.first, j.second);
      i->add_metrics_label(KSPP_KEY_TYPE_TAG, escape_influx(i->key_type_name()));
      i->add_metrics_label(KSPP_VALUE_TYPE_TAG, escape_influx(i->value_type_name()));

      //prometheus::Registry* xx = _prom_registry.get();

      for (auto &&j: i->get_metrics()) {
        j->finalize_labels(prom_registry_);
      }
    }
  }

  void topology::for_each_metrics(std::function<void(kspp::metric &)> f) {
    for (auto &&i: partition_processors_)
      for (auto &&j: i->get_metrics())
        f(*j);

    for (auto &&i: sinks_)
      for (auto &&j: i->get_metrics())
        f(*j);
  }

  void topology::init_processing_graph() {
    top_partition_processors_.clear();

    for (auto &&i: partition_processors_) {
      bool upstream_of_something = false;
      for (auto &&j: partition_processors_) {
        if (j->is_upstream(i.get()))
          upstream_of_something = true;
      }
      if (!upstream_of_something) {
        DLOG(INFO) << "topology << " << topology_id_ << ": adding " << i->log_name() << " to top";
        top_partition_processors_.push_back(i);
      } else {
        DLOG(INFO) << "topology << " << topology_id_ << ": skipping poll of " << i->log_name();
      }
    }
  }

  void topology::start(start_offset_t offset) {
    LOG_IF(FATAL, is_started_) << "usage error - started twice";

    if (cluster_config_->has_feature(cluster_config::KAFKA)) {
      if (offset == kspp::OFFSET_STORED)
        precondition_consumer_group_ = cluster_config_->get_consumer_group();

      for (auto &&i: partition_processors_) {
        auto topic = i->precondition_topic();
        if (topic.size())
          precondition_topics_.insert(topic);
      }

      for (auto &&i: sinks_) {
        auto topic = i->precondition_topic();
        if (topic.size())
          precondition_topics_.insert(topic);
      }
    }

    init_metrics();

    validate_preconditions();

    init_processing_graph();

    for (auto &&i: top_partition_processors_)
      i->start(offset);

    is_started_ = true;
  }


  void topology::validate_preconditions() {
    LOG(INFO) << "validating preconditions:  STARTING";
    if (precondition_consumer_group_.size()) {
      kspp::kafka::wait_for_consumer_group(cluster_config_, precondition_consumer_group_,
                                           cluster_config_->get_cluster_state_timeout());
    }

    for (auto &&i: precondition_topics_) {
      kspp::kafka::require_topic_leaders(cluster_config_, i);
    }
    LOG(INFO) << "validating preconditions:  DONE";
  }


  bool topology::eof() {
    for (auto &&i: top_partition_processors_) {
      if (!i->eof())
        return false;
    }
    for (auto &&i: partition_processors_) {
      if (!i->eof())
        return false;
    }

    return true;
  }

  bool topology::good() const {
    bool good = true;
    for (auto &&i: partition_processors_)
      if (!i->good()) {
        LOG(INFO) << "processor: " << i->log_name() << " good()==false";
        good = false;
      }

    for (auto &&i: sinks_)
      if (!i->good()) {
        LOG(INFO) << "processor: " << i->log_name() << " good()==false";
        good = false;
      }
    return good;
  }

  std::size_t topology::process(int64_t ts) {
    auto ev_count = 0u;
    if (allow_commit_chain_gc_)
      autocommit_marker_gc(); // this is a global resource - we have to do this better for now only allow

    // this needs to be done to to trigger callbacks
    for (auto &&i: sinks_)
      i->poll(0);

    // some of those might be kafka_partition_sinks....
    for (auto &&i: partition_processors_)
      i->poll(0);

    if (ts > next_gc_ts_) {
      for (auto &&i: partition_processors_)
        i->garbage_collect(ts);
      for (auto &&i: sinks_)
        i->garbage_collect(ts);
      next_gc_ts_ = ts + 10000; // 10 sec
    }

    // tbd partiotns sinks???
    // check sinks here an return 0 if we need to wait...
    // we should not check every loop
    // check every 1000 run?
    size_t sink_queue_len = 0;
    for (auto &&i: sinks_)
      sink_queue_len += i->outbound_queue_len();
    if (sink_queue_len > max_pending_sink_messages_)
      return 0;

    //int64_t tick = milliseconds_since_epoch();


    for (auto &&i: top_partition_processors_) {
      ev_count += i->process(ts);
      if (ev_count > 10000000)
        LOG(INFO) << "bad count: " << ev_count << ", " << i->log_name();
    }

    for (auto &&i: sinks_) {
      ev_count += i->process(ts);
      i->poll(0);
    }

    return ev_count;
  }

  std::size_t topology::process_1s() {
    if (allow_commit_chain_gc_)
      autocommit_marker_gc();
    for (auto &&i: sinks_)
      i->poll(0);
    for (auto &&i: partition_processors_)
      i->poll(0);

    size_t sink_queue_len = 0;
    for (auto &&i: sinks_)
      sink_queue_len += i->outbound_queue_len();
    if (sink_queue_len > max_pending_sink_messages_)
      return 0;

    int64_t min_ts = INT64_MAX;
    for (auto &&i: partition_processors_)
      min_ts = std::min(min_ts, i->next_event_time());

    // empty queues?
    if (min_ts == INT64_MAX)
      return 0;

    int64_t max_ts = std::min(min_ts + 1000, kspp::milliseconds_since_epoch() - min_buffering_ms_);

    size_t ev_count = 0;

    for (auto &&i: sinks_)
      ev_count += i->process(max_ts);

    for (int64_t ts = min_ts; ts < max_ts; ++ts)
      for (auto &&i: partition_processors_)
        ev_count += i->process(ts);

    for (auto &&i: sinks_)
      ev_count += i->process(max_ts);

    if (max_ts > next_gc_ts_) {
      for (auto &&i: partition_processors_)
        i->garbage_collect(max_ts);
      for (auto &&i: sinks_)
        i->garbage_collect(max_ts);
      next_gc_ts_ = max_ts + 10000; // 10 sec
    }

    return ev_count;
  }

  std::size_t topology::process_1ms() {
    if (allow_commit_chain_gc_)
      autocommit_marker_gc();
    for (auto &&i: sinks_)
      i->poll(0);
    for (auto &&i: partition_processors_)
      i->poll(0);


    int64_t now = kspp::milliseconds_since_epoch();
    if (now > next_gc_ts_) {
      for (auto &&i: partition_processors_)
        i->garbage_collect(now);
      for (auto &&i: sinks_)
        i->garbage_collect(now);
      next_gc_ts_ = now + 10000; // 10 sec
    }

    size_t sink_queue_len = 0;
    for (auto &&i: sinks_)
      sink_queue_len += i->outbound_queue_len();
    if (sink_queue_len > max_pending_sink_messages_)
      return 0;

    int64_t min_ts = INT64_MAX;
    for (auto &&i: partition_processors_)
      min_ts = std::min(min_ts, i->next_event_time());

    // empty queues?
    if (min_ts == INT64_MAX)
      return 0;

    int64_t max_ts = std::min(min_ts + 1, kspp::milliseconds_since_epoch() - min_buffering_ms_);

    size_t ev_count = 0;

    for (auto &&i: sinks_)
      ev_count += i->process(max_ts);

    for (int64_t ts = min_ts; ts < max_ts; ++ts)
      for (auto &&i: partition_processors_)
        ev_count += i->process(ts);

    for (auto &&i: sinks_)
      ev_count += i->process(max_ts);

    return ev_count;
  }

  void topology::close() {
    for (auto &&i: partition_processors_) {
      i->close();
    }
    for (auto &&i: sinks_) {
      i->close();
    }
  }

  void topology::commit(bool force) {
    LOG_IF(FATAL, !is_started_) << "usage error - commit without start()";
    for (auto &&i: top_partition_processors_)
      i->commit(force);
  }

// this is probably not enough if we have a topology that consists of
// sources -> sink -> source -> processing > sink -> source
// we might have stuff in sinks that needs to be flushed before processing in following steps can finish
// TBD how to capture this??
// for now we start with a flush of the sinks but that is not enough
  void topology::flush(bool wait_for_events, std::size_t event_limit) {
    LOG_IF(FATAL, !is_started_) << "usage error - flush without start()";

    auto processed = 0u;
    while (processed < event_limit) {
      autocommit_marker_gc();
      for (auto &&i: sinks_)
        i->flush();

      auto sz = process(milliseconds_since_epoch());

      if (sz) {
        processed += sz;
        continue;
      }

      if (sz == 0 && !eof() && wait_for_events)
        std::this_thread::sleep_for(10ms);
      else
        break;
    }

    for (auto &&i: top_partition_processors_) {
      autocommit_marker_gc();
      i->flush();
    }

    while (processed < event_limit) {
      autocommit_marker_gc();
      for (auto &&i: sinks_) {
        i->flush();
        autocommit_marker_gc();
      }

      auto sz = process(milliseconds_since_epoch());

      if (sz) {
        processed += sz;
        continue;
      }

      if (sz == 0 && !eof() && wait_for_events)
        std::this_thread::sleep_for(10ms);
      else
        break;
    }
  }
}


