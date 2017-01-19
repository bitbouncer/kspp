#include <cstdlib>
#include <boost/filesystem.hpp>
#include <boost/log/trivial.hpp>
#include "processors/join.h"
#include "processors/count.h"
#include "processors/repartition.h"
#include "processors/filter.h"
#include "processors/transform.h"
#include "processors/rate_limiter.h"
#include "impl/kstream_impl.h"
#include "impl/ktable_impl.h"
#include "sinks/kafka_sink.h"
#include "sinks/stream_sink.h"
#include "sources/kafka_source.h"

#pragma once


// maybee this is not good at all - if we have a separate processors that uses it's own thread to call 
// process one we relly dont want those to be mixed.... TBD
//class topoplogy
//{
//  public:
//  topoplogy() {}
//
//  void add(std::shared_ptr<partition_processor> p) {
//    std::lock_guard<std::mutex> lock(_mutex);
//    _partition_processors.push_back(p);
//  }
//
//  void add(std::shared_ptr<topic_processor> p) {
//    std::lock_guard<std::mutex> lock(_mutex);
//    _topic_processors.push_back(p);
//  }
//
//  bool start() {}
//  bool process_init_eof() { return true; }
//  bool process_init() { return false; }
//  bool eof() { return true; }
//  bool process() { return false; }
//  protected:
//  std::mutex                                        _mutex;
//  std::vector<std::shared_ptr<partition_processor>> _partition_processors;
//  std::vector<std::shared_ptr<topic_processor>>     _topic_processors;
//};


namespace kspp {
template<class CODEC>
class topology
{
  public:
  topology(std::string app_id, std::string topology_id, std::string brokers, boost::filesystem::path root_path, std::shared_ptr<CODEC> default_codec)
    : _app_id(app_id)
    , _topology_id(topology_id)
    , _brokers(brokers)
    , _default_codec(default_codec)
    , _root_path(root_path) {
    BOOST_LOG_TRIVIAL(info) << "topology created, name:" << name() << ", brokers:" << brokers << " , storage_path:" << root_path;
  }

  ~topology() {
    BOOST_LOG_TRIVIAL(info) << "topology, name:" << name() << " terminated";
    // output stats
  }

  std::string name() const {
    return _app_id + "__" + _topology_id;
  }

  inline std::shared_ptr<CODEC> codec() {
    return _default_codec;
  }

  void init_metrics() {
    for (auto i : _partition_processors) {
      for (auto j : i->get_metrics()) {
        j->_logged_name = _app_id + "." + _topology_id + ".depth-" + std::to_string(i->depth()) + "." + i->processor_name() + ".partition-" + std::to_string(i->partition()) + "." + j->_simple_name;
      }
    }

    for (auto i : _topic_processors) {
      for (auto j : i->get_metrics()) {
        j->_logged_name = _app_id + "." + _topology_id + "." + i->processor_name() + "." + j->_simple_name;
        //j->_logged_name = _app_id + "." + _topology_id + ".depth-" + std::to_string(i->depth()) + "." + i->processor_name() + "." + j->_simple_name;
      }
    }

  }

  void output_metrics(std::ostream& s) {
    for (auto i : _partition_processors) {
      for (auto j : i->get_metrics())
        s << "metrics: " << j->name() << " : " << j->value() << std::endl;
    }
    
    for (auto i : _topic_processors) {
      for (auto j : i->get_metrics()) {
        s << "metrics: " << j->name() << " : " << j->value() << std::endl;
      }
    }
  }

  // TBD this should only call most downstream processors??
  // or should topology call all members??
  void start(int offset) {
    for (auto i : _partition_processors)
      i->start(offset);
  }

  // TBD this should only call most downstream processors??
  void flush() {
    for (auto i : _partition_processors)
      i->flush();
  }

  /* TBD
  void collect_metrics(merics - sink*) {
  // figure out which is upstream
  // add seq number to processors...
  //
  }
  */

  //std::shared_ptr<left_join<K, streamV, tableV, R>> create_left_join(std::shared_ptr<kspp::partition_source<K, streamV>> right, std::shared_ptr<kspp::ktable_partition<K, tableV>> left, typename kspp::left_join<K, streamV, tableV, R>::value_joiner value_joiner) {
  template<class K, class V>
  std::shared_ptr<kspp::partition_source<K, V>> create_mem_source(/* upstream and partition*/) {
    //auto p = std::make_shared<kspp::create_mem_source<K, V>>(source, partition, get_storage_path(), _default_codec);
    //_partition_processors.push_back(p);
    return NULL;
  }

  template<class K, class streamV, class tableV, class R>
  std::shared_ptr<kspp::partition_source<K, R>> create_left_join(std::shared_ptr<kspp::partition_source<K, streamV>> right, std::shared_ptr<kspp::ktable_partition<K, tableV>> left, typename kspp::left_join<K, streamV, tableV, R>::value_joiner value_joiner) {
    auto p = kspp::left_join<K, streamV, tableV, R>::create(right, left, value_joiner);
    _partition_processors.push_back(p);
    return p;
  }

  template<class K, class V, class PK>
  std::shared_ptr<partition_processor> create_repartition(std::shared_ptr<kspp::partition_source<K, V>> source, std::shared_ptr<kspp::ktable_partition<K, PK>> left, std::shared_ptr<topic_sink<K, V, CODEC>> topic_sink) {
    auto p = kspp::repartition_by_table<K, V, PK, CODEC>::create(source, left, topic_sink);
    _partition_processors.push_back(p);
    return p;
  }

  //TBD we shouyld get rid of void value - we do not require that but how do we tell compiler that????
  template<class K, class V>
  std::shared_ptr<kspp::materialized_partition_source<K, V>> create_count_by_key(std::shared_ptr<partition_source<K, void>> source, int64_t punctuate_intervall) {
    auto p = kspp::count_by_key<K, V, CODEC>::create(source, get_storage_path(), punctuate_intervall, _default_codec);
    _partition_processors.push_back(p);
    return p;
  }

  template<class K, class V>
  std::vector<std::shared_ptr<kspp::materialized_partition_source<K, V>>> create_count_by_key(std::vector<std::shared_ptr<partition_source<K, void>>>& sources, int64_t punctuate_intervall) {
    std::vector<std::shared_ptr<kspp::materialized_partition_source<K, V>>> res;
    for (auto i : sources)
      res.push_back(create_count_by_key<K, V>(i, punctuate_intervall));
    return res;
  }

  /**
  creates a kafka sink using default partitioner (hash on key)
  */
  template<class K, class V>
  std::shared_ptr<kspp::topic_sink<K, V, CODEC>> create_kafka_sink(std::string topic) {
    auto p = kspp::kafka_sink<K, V, CODEC>::create(_brokers, topic, _default_codec);
    _topic_processors.push_back(p);
    return p;
  }
  
  /**
  creates a kafka sink using explicit partitioner
  */
  template<class K, class V>
  std::shared_ptr<kspp::topic_sink<K, V, CODEC>> create_kafka_sink(std::string topic, std::function<uint32_t(const K& key)> partitioner) {
    auto p = kspp::kafka_sink<K, V, CODEC>::create(_brokers, topic, partitioner, _default_codec);
    _topic_processors.push_back(p);
    return p;
  }

  /**
  creates a kafka sink using explicit partition
  */
  template<class K, class V>
  std::shared_ptr<kspp::partition_sink<K, V>> create_kafka_sink(std::string topic, size_t partition) {
    auto p = kspp::kafka_single_partition_sink<K, V, CODEC>::create(_brokers, topic, partition, _default_codec);
    _partition_processors.push_back(p);
    return p;
  }

  template<class K, class V>
  std::shared_ptr<kspp::partition_sink<K, V>> create_global_kafka_sink(std::string topic) {
    auto p = kspp::kafka_single_partition_sink<K, V, CODEC>::create(_brokers, topic, 0, _default_codec);
    _partition_processors.push_back(p);
    return p;
  }

  template<class K, class V>
  std::shared_ptr<kspp::partition_source<K, V>> create_kafka_source(std::string topic, size_t partition) {
    auto p = std::make_shared<kspp::kafka_source<K, V, CODEC>>(_brokers, topic, partition, _default_codec);
    _partition_processors.push_back(p);
    return p;
  }

  template<class K, class V>
  std::vector<std::shared_ptr<kspp::partition_source<K, V>>> create_kafka_sources(std::string topic, size_t nr_of_partitions) {
    std::vector<std::shared_ptr<kspp::partition_source<K, V>>> v;
    for (size_t i = 0; i != nr_of_partitions; ++i)
      v.push_back(create_kafka_source<K, V>(topic, i));
    return v;
  }

  template<class K, class V>
  std::shared_ptr<kspp::kstream_partition<K, V>> create_kstream(std::string topic, size_t partition) { // TBD tags for application_id and processor_id (append to storage paths??)
    auto p = std::make_shared<kspp::kstream_partition_impl<K, V, CODEC>>(_brokers, topic, partition, get_storage_path(), _default_codec);
    _partition_processors.push_back(p);
    return p;
  }

  template<class K, class V>
  std::shared_ptr<kspp::ktable_partition<K, V>> create_ktable(std::string topic, size_t partition) {
    // here we should creeate a own kafka source and use the below kspp::partition_source<K, V> source variant...
    auto p = std::make_shared<kspp::ktable_partition_impl<K, V, CODEC>>(_brokers, topic, partition, get_storage_path(), _default_codec);
    _partition_processors.push_back(p);
    return p;
  }

  //template<class K, class V>
  //std::shared_ptr<kspp::ktable_partition<K, V>> create_ktable(kspp::partition_source<K, V> source, size_t partition) {
  //  auto p = std::make_shared<kspp::ktable_partition_impl<K, V, CODEC>>(source, partition, get_storage_path(), _default_codec);
  //  _partition_processors.push_back(p);
  //  return p;
  //}

  template<class K, class V>
  std::shared_ptr<kspp::ktable_partition<K, V>> create_global_ktable(std::string topic) {
    auto p = std::make_shared<kspp::ktable_partition_impl<K, V, CODEC>>(_brokers, topic, 0, get_storage_path(), _default_codec);
    _partition_processors.push_back(p);
    return p;
  }

  template<class K, class V>
  std::shared_ptr<kspp::stream_sink<K, V>> create_stream_sink(std::shared_ptr<kspp::partition_source<K, V>>source, std::ostream& os) {
    auto p = kspp::stream_sink<K, V>::create(source, os);
    return p;
  }

  /* ask DAG!!!
  template<class PARTITION_SOURCE>
  std::shared_ptr<kspp::stream_sink<typename PARTITION_SOURCE::key_type, typename PARTITION_SOURCE::value_type>> create_stream_sink(std::shared_ptr<PARTITION_SOURCE> source, std::ostream& os) {
  auto p = std::make_shared<kspp::stream_sink<typename PARTITION_SOURCE::key_type, typename PARTITION_SOURCE::value_type>>(source, os);
  //_topology.push_back(p);
  return p;
  }
  */


  // not useful for anything excepts cout ord cerr... since everything is bundeled into same stream???
  // maybee we should have a topicstreamsink that takes a vector intead...
  template<class K, class V>
  std::vector<std::shared_ptr<kspp::stream_sink<K, V>>> create_stream_sinks(std::vector<std::shared_ptr<kspp::partition_source<K, V>>> sources, std::ostream& os) {
    std::vector<std::shared_ptr<kspp::stream_sink<K, V>>> v;
    for (auto s : sources)
      v.push_back(create_stream_sink<K, V>(s, os));
    return v;
  }

  template<class K, class V>
  std::shared_ptr<kspp::partition_source<K, V>> create_filter(std::shared_ptr<kspp::partition_source<K, V>> source, typename kspp::filter<K, V>::predicate f) {
    auto p = kspp::filter<K, V>::create(source, f);
    _partition_processors.push_back(p);
    return p;
  }

  template<class SK, class SV, class RK, class RV>
  std::shared_ptr<partition_source<RK, RV>> create_flat_map(std::shared_ptr<kspp::partition_source<SK, SV>> source, typename kspp::flat_map<SK, SV, RK, RV>::extractor f) {
    auto p = flat_map<SK, SV, RK, RV>::create(source, f);
    _partition_processors.push_back(p);
    return p;
  }

  template<class K, class SV, class RV>
  std::shared_ptr<partition_source<K, RV>> create_transform_value(std::shared_ptr<kspp::partition_source<K, SV>> source, typename kspp::transform_value<K, SV, RV>::extractor f) {
    auto p = transform_value<K, SV, RV>::create(source, f);
    _partition_processors.push_back(p);
    return p;
  }

  template<class K, class V>
  std::shared_ptr<kspp::partition_source<K, V>> create_rate_limiter(std::shared_ptr<kspp::partition_source<K, V>> source, int64_t agetime, size_t capacity) {
    auto p = rate_limiter<K, V>::create(source, agetime, capacity);
    _partition_processors.push_back(p);
    return p;
  }

  template<class K, class V>
  std::shared_ptr<kspp::partition_source<K, V>> create_thoughput_limiter(std::shared_ptr<kspp::partition_source<K, V>> source, double messages_per_sec) {
    auto p = thoughput_limiter<K, V>::create(source, messages_per_sec);
    _partition_processors.push_back(p);
    return p;
  }




  private:
  boost::filesystem::path get_storage_path() {
    boost::filesystem::path top_of_topology(_root_path);
    top_of_topology /= _app_id;
    top_of_topology /= _topology_id;
    BOOST_LOG_TRIVIAL(debug) << "topology << " << name() << ": creating local storage at " << top_of_topology;
    boost::filesystem::create_directories(top_of_topology);
    return top_of_topology;
  }

  private:
  std::string                                       _app_id;
  std::string                                       _topology_id;
  std::string                                       _brokers;
  std::shared_ptr<CODEC>                            _default_codec;
  boost::filesystem::path                           _root_path;
  std::vector<std::shared_ptr<partition_processor>> _partition_processors;
  std::vector<std::shared_ptr<topic_processor>>     _topic_processors;
};

template<class CODEC>
class topology_builder
{
  public:
  static boost::filesystem::path default_directory() {
    if (const char* env_p = std::getenv("KSPP_STATE_DIR")) {
      return boost::filesystem::path(env_p);
    }
    return boost::filesystem::temp_directory_path();
  }

  static std::string default_kafka_broker() {
    if (const char* env_p = std::getenv("KAFKA_BROKER"))
      return std::string(env_p);
    return "localhost";
  }

  topology_builder(std::string app_id, std::string brokers= default_kafka_broker(), boost::filesystem::path root_path = default_directory(), std::shared_ptr<CODEC> default_codec = std::make_shared<CODEC>())
    : _app_id(app_id)
    , _next_topology_id(0)
    , _brokers(brokers)
    , _default_codec(default_codec)
    , _root_path(root_path) {
    BOOST_LOG_TRIVIAL(info) << "topology_builder created, app_id:" << app_id << ", kafka_brokers:" << brokers << ", root_path:" << root_path;
  }

  std::shared_ptr<topology<CODEC>> create_topology(std::string id) {
    return std::make_shared<topology<CODEC>>(_app_id, id, _brokers, _root_path, _default_codec);
  }

  std::shared_ptr<topology<CODEC>> create_topology() {
    std::string id = "topology-" + std::to_string(_next_topology_id);
    _next_topology_id++;
    return std::make_shared<topology<CODEC>>(_app_id, id, _brokers, _root_path, _default_codec);
  }

  inline std::shared_ptr<CODEC> codec() {
    return _default_codec;
  }

  private:
  std::string             _app_id;
  std::string             _brokers;
  std::shared_ptr<CODEC>  _default_codec;
  boost::filesystem::path _root_path;
  size_t                  _next_topology_id;
};




};
