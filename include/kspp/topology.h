#include <kspp/kspp.h>
#include <kspp/processors/merge.h>
#include <limits>
#include <set>
#include <prometheus/registry.h>
#pragma once

namespace kspp {
  class topology {
  public:
    topology(std::shared_ptr<cluster_config> c_config, std::string topology_id, bool internal=false);

    virtual ~topology();

    std::shared_ptr<cluster_config> get_cluster_config() {
      return _cluster_config;
    }

    std::chrono::milliseconds max_buffering_time() const;

    void add_labels(const std::map<std::string, std::string>& labels){
      _labels.insert(labels.begin(), labels.end());
    }

    void for_each_metrics(std::function<void(kspp::metric &)> f);

    bool good() const;

    bool eof();

    std::size_t process_1s();
    std::size_t process_1ms();
    std::size_t process(int64_t ts); // =milliseconds_since_epoch()

    void close();

    void start(start_offset_t offset);

    void commit(bool force);

    void flush(bool wait_for_events = true, std::size_t event_limit = std::numeric_limits<std::size_t>::max());

    void validate_preconditions();

    // top level factory
    template<class pp, typename... Args>
    typename std::enable_if<std::is_base_of<kspp::partition_processor, pp>::value, std::vector<std::shared_ptr<pp>>>::type
    create_processors(std::vector<int> partition_list, Args... args) {
      std::vector <std::shared_ptr<pp>> result;
      for (auto i : partition_list) {
        auto p = std::make_shared<pp>(this->get_cluster_config(), i, args...);
        _partition_processors.push_back(p);
        result.push_back(p);
      }
      return result;
    }

    // should this be removed?? right now only merge
    template<class pp, typename... Args>
    typename std::enable_if<std::is_base_of<kspp::partition_processor, pp>::value, std::shared_ptr<pp>>::type
    create_processor(Args... args) {
      auto p = std::make_shared<pp>(this->get_cluster_config(), args...);
      _partition_processors.push_back(p);
      return p;
    }

    // when you have a vector of partitions - lets create a new processor layer
    template<class pp, class ps, typename... Args>
    typename std::enable_if<std::is_base_of<kspp::partition_processor, pp>::value, std::vector<std::shared_ptr<pp>>>::type
    create_processors(std::vector<std::shared_ptr<ps>> sources, Args... args) {
      std::vector <std::shared_ptr<pp>> result;
      for (auto i : sources) {
        auto p = std::make_shared<pp>(this->get_cluster_config(), i, args...);
        _partition_processors.push_back(p);
        result.push_back(p);
      }
      return result;
    }

    /**
      joins between two arrays
      we could probably have stricter contraint on the types of v1 and v2
    */
    template<class pp, class sourceT, class leftT, typename... Args>
    typename std::enable_if<std::is_base_of<kspp::partition_processor, pp>::value, std::vector<std::shared_ptr<pp>>>::type
    create_processors(
        std::vector<std::shared_ptr<sourceT>> v1,
        std::vector<std::shared_ptr<leftT>> v2,
        Args... args) {
      std::vector <std::shared_ptr<pp>> result;
      auto i = v1.begin();
      auto j = v2.begin();
      auto end = v1.end();
      for (; i != end; ++i, ++j) {
        auto p = std::make_shared<pp>(this->get_cluster_config(), *i, *j, args...);
        _partition_processors.push_back(std::static_pointer_cast<kspp::partition_processor>(p));
        result.push_back(p);
      }
      return result;
    }

    // TBD
    // only kafka metrics reporter uses this - fix this by using a stream and a separate sink or raw sink
    template<class pp, typename... Args>
    typename std::enable_if<std::is_base_of<kspp::processor, pp>::value, std::shared_ptr<pp>>::type
    create_sink(Args... args) {
      auto p = std::make_shared<pp>(this->get_cluster_config(), args...);
      _sinks.push_back(p);
      return p;
    }

    // create from single source - return one (kafka sink)
    template<class pp, class source, typename... Args>
    typename std::enable_if<std::is_base_of<kspp::processor, pp>::value, std::shared_ptr<pp>>::type
    create_sink(std::shared_ptr<source> src, Args... args) {
      auto p = std::make_shared<pp>(this->get_cluster_config(), args...);
      _sinks.push_back(p);
      src->add_sink(p);
      return p;
    }

    // create from vector of sources - return one (kafka sink..)
    template<class pp, class source, typename... Args>
    typename std::enable_if<std::is_base_of<kspp::processor, pp>::value, std::shared_ptr<pp>>::type
    create_sink(std::vector<std::shared_ptr<source>> sources, Args... args) {
      auto p = std::make_shared<pp>(this->get_cluster_config(), args...);
      _sinks.push_back(p);
      for (auto i : sources)
        i->add_sink(p);
      return p;
    }

    std::shared_ptr<prometheus::Registry> get_prometheus_registry() {
      return _prom_registry;
    }

    prometheus::Counter& metrics_counter_add(std::string what, metric::mtype t, std::string unit, const std::map<std::string, std::string>& labels){
      auto& counter_family = prometheus::BuildCounter().Name("kspp_" + what).Labels(_labels).Register(*_prom_registry);
      std::map<std::string, std::string> l(labels);
      l["unit"]=unit;
      return counter_family.Add(l);
    }

  protected:
    void init_metrics();
    void init_processing_graph();
    bool _is_started;
    std::shared_ptr<cluster_config> _cluster_config;
    std::string _topology_id;
    boost::filesystem::path _root_path;
    std::vector<std::shared_ptr<partition_processor>> _partition_processors;
    std::vector<std::shared_ptr<processor>> _sinks;
    std::vector<std::shared_ptr<partition_processor>> _top_partition_processors;
    int64_t _next_gc_ts;
    int64_t _min_buffering_ms;
    size_t _max_pending_sink_messages;
    std::set<std::string> _precondition_topics;
    std::string _precondition_consumer_group;
    bool _allow_commit_chain_gc=true;

    std::map<std::string, std::string> _labels;
    std::shared_ptr<prometheus::Registry> _prom_registry;
  };
} // namespace
