#include <kspp/kspp.h>
#include <set>
#pragma once

namespace kspp {
  class topology {
  public:
    topology(std::shared_ptr<app_info> ai,
             std::shared_ptr<cluster_config> c_config,
             std::string topology_id);

    virtual ~topology();

    std::shared_ptr<cluster_config> get_cluster_config() {
      return _cluster_config;
    }

    void set_storage_path(boost::filesystem::path root_path);

    std::string app_id() const;

    std::string consumer_group() const;

    std::string topology_id() const;

    std::string brokers() const;

    std::string name() const;

    std::chrono::milliseconds max_buffering_time() const;

    void init_metrics();

    void for_each_metrics(std::function<void(kspp::metric &)> f);

    bool eof();

    int process_one();

    void close();

    void start(start_offset_t offset);

    void commit(bool force);

    void flush();

    boost::filesystem::path get_storage_path();

    void validate_preconditions();

    // top level factory
    template<class pp, typename... Args>
    typename std::enable_if<std::is_base_of<kspp::partition_processor, pp>::value, std::vector<std::shared_ptr<pp>>>::type
    create_processors(std::vector<int> partition_list, Args... args) {
      std::vector <std::shared_ptr<pp>> result;
      for (auto i : partition_list) {
        auto p = std::make_shared<pp>(*this, i, args...);
        _partition_processors.push_back(p);
        result.push_back(p);
      }
      return result;
    }

    // should this be removed?? right now only pipe support this (ie merge)
    template<class pp, typename... Args>
    typename std::enable_if<std::is_base_of<kspp::partition_processor, pp>::value, std::shared_ptr<pp>>::type
    create_processor(Args... args) {
      auto p = std::make_shared<pp>(*this, args...);
      _partition_processors.push_back(p);
      return p;
    }

    template<class ps, typename... Args>
    std::shared_ptr<kspp::merge<typename ps::key_type, typename ps::value_type>>
    merge(std::vector<std::shared_ptr<ps>> sources, Args... args) {
      std::shared_ptr <kspp::merge<typename ps::key_type, typename ps::value_type>> result = std::make_shared<kspp::merge<typename ps::key_type, typename ps::value_type>>(
          *this, sources, args...);
      _partition_processors.push_back(result);
      return result;
    }

    // when you have a vector of partitions - lets create a new processor layer
    template<class pp, class ps, typename... Args>
    typename std::enable_if<std::is_base_of<kspp::partition_processor, pp>::value, std::vector<std::shared_ptr<pp>>>::type
    create_processors(std::vector<std::shared_ptr<ps>> sources, Args... args) {
      std::vector <std::shared_ptr<pp>> result;
      for (auto i : sources) {
        auto p = std::make_shared<pp>(*this, i, args...);
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
        auto p = std::make_shared<pp>(*this, *i, *j, args...);
        _partition_processors.push_back(std::static_pointer_cast<kspp::partition_processor>(p));
        result.push_back(p);
      }
      return result;
    }

    // this seems to be only sinks???
    template<class pp, typename... Args>
    typename std::enable_if<std::is_base_of<kspp::processor, pp>::value, std::shared_ptr<pp>>::type
    create_sink(Args... args) {
      auto p = std::make_shared<pp>(*this, args...);
      _sinks.push_back(p);
      return p;
    }

    // this seems to be only sinks??? create from vector of sources - return one (sounds like merge??? exept merge is also source)
    template<class pp, class source, typename... Args>
    typename std::enable_if<std::is_base_of<kspp::processor, pp>::value, std::shared_ptr<pp>>::type
    create_sink(std::vector<std::shared_ptr<source>> sources, Args... args) {
      auto p = std::make_shared<pp>(*this, args...);
      _sinks.push_back(p);
      for (auto i : sources)
        i->add_sink(p);
      return p;
    }

  protected:
    void init_processing_graph();
    bool _is_started;

    std::shared_ptr<app_info> _app_info;
    std::shared_ptr<cluster_config> _cluster_config;
    std::string _topology_id;
    boost::filesystem::path _root_path;
    std::vector<std::shared_ptr<partition_processor>> _partition_processors;
    std::vector<std::shared_ptr<processor>> _sinks;
    std::vector<std::shared_ptr<partition_processor>> _top_partition_processors;
    int64_t _next_gc_ts;

    std::set<std::string> _precondition_topics;
    std::string _precondition_consumer_group;
  };
} // namespace