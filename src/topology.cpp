#include <kspp/topology.h>
#include <kspp/kspp.h>
#include <kspp/utils/kafka_utils.h>
#include <algorithm>

using namespace std::chrono_literals;

namespace kspp {
  topology::topology(std::shared_ptr<app_info> ai,
                     std::shared_ptr<cluster_config> cc,
                     std::string topology_id)
      : _app_info(ai)
      , _cluster_config(cc)
      , _is_started(false)
      , _topology_id(topology_id)
      , _next_gc_ts(0)
      , _min_buffering_ms(cc->get_min_topology_buffering().count())
      , _max_pending_sink_messages(cc->get_max_pending_sink_messages()) {
    LOG(INFO) << "topology created, name:" << name();
  }

  topology::~topology() {
    LOG(INFO) << "topology, name:" << name() << " terminating";
    _top_partition_processors.clear();
    _partition_processors.clear();
    _sinks.clear();
    LOG(INFO) << "topology, name:" << name() << " terminated";
  }

  std::string topology::app_id() const {
    return _app_info->identity();
  }

  std::string topology::consumer_group() const {
    return _app_info->consumer_group();
  }

  std::string topology::topology_id() const {
    return _topology_id;
  }

  std::string topology::name() const {
    return "[" + _app_info->identity() + "]#" + _topology_id;
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

  void topology::init_metrics(std::vector<metrics20::avro::metrics20_key_tags_t> app_tags) {
    for (auto &&i : _partition_processors) {
      for (auto j : app_tags)
        i->add_metrics_tag(j.key, j.value);

      i->add_metrics_tag(KSPP_KEY_TYPE_TAG, escape_influx(i->key_type_name()));
      i->add_metrics_tag(KSPP_VALUE_TYPE_TAG, escape_influx(i->value_type_name()));
      i->add_metrics_tag(KSPP_PARTITION_TAG, std::to_string(i->partition()));

      for (auto &&j : i->get_metrics()) {
        j->finalize_tags(); // maybe add string escape function here...
      }
    }

    for (auto &&i : _sinks) {
      for (auto j : app_tags)
        i->add_metrics_tag(j.key, j.value);
      i->add_metrics_tag(KSPP_KEY_TYPE_TAG, escape_influx(i->key_type_name()));
      i->add_metrics_tag(KSPP_VALUE_TYPE_TAG, escape_influx(i->value_type_name()));

      for (auto &&j : i->get_metrics()) {
        j->finalize_tags();
      }
    }
  }

  void topology::for_each_metrics(std::function<void(kspp::metric &)> f) {
    for (auto &&i : _partition_processors)
      for (auto &&j : i->get_metrics())
        f(*j);

    for (auto &&i : _sinks)
      for (auto &&j : i->get_metrics())
        f(*j);
  }

  void topology::init_processing_graph() {
    _top_partition_processors.clear();

    for (auto &&i : _partition_processors) {
      bool upstream_of_something = false;
      for (auto &&j : _partition_processors) {
        if (j->is_upstream(i.get()))
          upstream_of_something = true;
      }
      if (!upstream_of_something) {
        DLOG(INFO) << "topology << " << name() << ": adding " << i->log_name() << " to top";
        _top_partition_processors.push_back(i);
      } else {
        DLOG(INFO) << "topology << " << name() << ": skipping poll of " << i->log_name();
      }
    }
  }

  void topology::start(start_offset_t offset) {
    LOG_IF(FATAL, _is_started) << "usage error - started twice";

    if (offset == kspp::OFFSET_STORED)
      _precondition_consumer_group = consumer_group();

    for (auto &&i : _partition_processors){
      auto topic = i->precondition_topic();
      if (topic.size())
        _precondition_topics.insert(topic);
    }

    for (auto &&i : _sinks){
      auto topic = i->precondition_topic();
      if (topic.size())
        _precondition_topics.insert(topic);
    }

    validate_preconditions();

    init_processing_graph();

    for (auto &&i : _top_partition_processors)
      i->start(offset);

    _is_started = true;
  }


  void topology::validate_preconditions() {
    LOG(INFO) << "validating preconditions:  STARTING";
    if (_precondition_consumer_group.size())
    {
      kspp::kafka::wait_for_consumer_group(_cluster_config, _precondition_consumer_group, _cluster_config->get_cluster_state_timeout());
    }

    for (auto &&i : _precondition_topics){
      kspp::kafka::require_topic_leaders(_cluster_config, i);
    }
    LOG(INFO) << "validating preconditions:  DONE";
  }


  bool topology::eof() {
    for (auto &&i : _top_partition_processors) {
      if (!i->eof())
        return false;
    }
    return true;
  }

  std::size_t topology::process(int64_t ts) {
    auto ev_count = 0u;

    // this needs to be done to to trigger callbacks
    for (auto &&i : _sinks)
      i->poll(0);

    // some of those might be kafka_partition_sinks....
    for (auto &&i : _partition_processors)
      i->poll(0);

    // tbd partiotns sinks???
    // check sinks here an return 0 if we need to wait...
    // we should not check every loop
    // check every 1000 run?
    size_t sink_queue_len = 0;
    for (auto &&i : _sinks)
      sink_queue_len += i->outbound_queue_len();
    if (sink_queue_len > _max_pending_sink_messages)
      return 0;

    //int64_t tick = milliseconds_since_epoch();


    for (auto &&i : _top_partition_processors) {
      ev_count += i->process(ts);
    }

    for (auto &&i : _sinks)
      ev_count += i->process(ts);

    if (ts > _next_gc_ts) {
      for (auto &&i : _partition_processors)
        i->garbage_collect(ts);
      for (auto &&i : _sinks)
        i->garbage_collect(ts);
      _next_gc_ts = ts + 10000; // 10 sec
    }

    return ev_count;
  }

  std::size_t topology::process_1s(){
    for (auto &&i : _sinks)
      i->poll(0);
    for (auto &&i : _partition_processors)
      i->poll(0);

    size_t sink_queue_len = 0;
    for (auto &&i : _sinks)
      sink_queue_len += i->outbound_queue_len();
    if (sink_queue_len > _max_pending_sink_messages)
      return 0;

    int64_t min_ts = INT64_MAX;
    for (auto &&i : _partition_processors)
      min_ts = std::min(min_ts, i->next_event_time());


    int64_t max_ts = std::min(min_ts+1000, kspp::milliseconds_since_epoch()-_min_buffering_ms);

    size_t ev_count=0;

    for (auto &&i : _sinks)
      ev_count += i->process(max_ts);

    for (int64_t ts = min_ts; ts != max_ts; ++ts)
    for (auto &&i : _partition_processors)
      ev_count += i->process(ts);

    for (auto &&i : _sinks)
      ev_count +=  i->process(max_ts);

    if (max_ts > _next_gc_ts) {
      for (auto &&i : _partition_processors)
        i->garbage_collect(max_ts);
      for (auto &&i : _sinks)
        i->garbage_collect(max_ts);
      _next_gc_ts = max_ts + 10000; // 10 sec
    }

    return ev_count;
  }

  void topology::close() {
    for (auto &&i : _partition_processors) {
      i->close();
    }
    for (auto &&i : _sinks) {
      i->close();
    }
  }

  void topology::commit(bool force) {
    LOG_IF(FATAL, !_is_started) << "usage error - commit without start()";
    for (auto &&i : _top_partition_processors)
      i->commit(force);
  }

// this is probably not enough if we have a topology that consists of
// sources -> sink -> source -> processing > sink -> source
// we might have stuff in sinks that needs to be flushed before processing in following steps can finish
// TBD how to capture this??
// for now we start with a flush of the sinks but that is not enough
  void topology::flush(bool wait_for_events, std::size_t event_limit) {
    LOG_IF(FATAL, !_is_started) << "usage error - flush without start()";

    auto processed = 0u;
    while (processed < event_limit) {
      for (auto &&i : _sinks)
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

    for (auto &&i : _top_partition_processors)
      i->flush();

    while (processed < event_limit) {
      for (auto &&i : _sinks)
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
  }

  boost::filesystem::path topology::get_storage_path() {
    // if no storage path has been set - let an eventual state store handle this problem
    // only disk based one need th#include <vector>is and we pass storage path to all state stores (mem ones also)
    if (_cluster_config->get_storage_root().size()==0)
      return "";

    boost::filesystem::path top_of_topology(_cluster_config->get_storage_root());
    top_of_topology /= sanitize_filename(_app_info->identity());
    top_of_topology /= sanitize_filename(_topology_id);
    DLOG(INFO) << "topology " << _app_info->identity() << ": creating local storage at " << top_of_topology;
    auto res = boost::filesystem::create_directories(top_of_topology);
    // seems to be a bug in boost - always return false...
    // so we check if the directory exists after instead...
    if (!boost::filesystem::exists(top_of_topology))
      LOG(FATAL) << "topology " << _app_info->identity() << ": failed to create local storage at "
                 << top_of_topology;
    return top_of_topology;
  }
}


