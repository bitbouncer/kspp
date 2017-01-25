#include "impl/kstream_impl.h"
#include "impl/ktable_impl.h"
#include "sinks/kafka_sink.h"
#include "sinks/stream_sink.h"
#include "sources/kafka_source.h"
#include "processors/pipe.h"
#include "processors/join.h"
#include "processors/count.h"
#include "processors/repartition.h"
#include "processors/filter.h"
#include "processors/transform.h"
#include "processors/rate_limiter.h"
#include "codecs/text_codec.h"

#include <cstdlib>
#include <boost/filesystem.hpp>
#include <boost/log/trivial.hpp>



#pragma once

namespace kspp {
  template<class CODEC>
  class topology
  {
  public:
    topology(std::string app_id, std::string topology_id, int32_t partition, std::string brokers, boost::filesystem::path root_path, std::shared_ptr<CODEC> default_codec)
      : _app_id(app_id)
      , _is_init(false)
      , _topology_id(topology_id)
      , _partition(partition)
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
      return _app_id + "__" + _topology_id + "[p" + std::to_string(_partition) + "]";
    }

    inline std::shared_ptr<CODEC> codec() {
      return _default_codec;
    }

    // the metrics should look like this...
    //cpu_load_short, host=server01, region=us-west value=0.64 1434055562000000000
    //metric_name, add_id={app_id}}, topology={{_topology_id}}, depth={{depth}}, processor_type={{processor_name()}} record_type="
    void init_metrics() {
      for (auto i : _partition_processors) {
        for (auto j : i->get_metrics()) {
          j->_logged_name = j->_simple_name + ", app_id=" + _app_id + ", topology=" + _topology_id + ", depth=" + std::to_string(i->depth()) + ", processor_type=" + i->processor_name() + ", record_type='" + i->record_type_name() + "', partition=" + std::to_string(i->partition());
          std::cerr << j->_logged_name << std::endl;
          //j->_logged_name = _app_id + "." + _topology_id + ".depth-" + std::to_string(i->depth()) + "." + i->processor_name() + i->record_type_name() + ".partition-" + std::to_string(i->partition()) + "." + j->_simple_name;
        }
      }

      for (auto i : _topic_processors) {
        for (auto j : i->get_metrics()) {
          j->_logged_name = j->_simple_name + ", app_id=" + _app_id + ", topology=" + _topology_id + ", processor_type=" + i->processor_name() + ", record_type='" + i->record_type_name() + "'";
          //j->_logged_name = _app_id + "." + _topology_id + "." + i->processor_name() + i->record_type_name() + "." + j->_simple_name;
          //j->_logged_name = _app_id + "." + _topology_id + ".depth-" + std::to_string(i->depth()) + "." + i->processor_name() + "." + j->_simple_name;
        }
      }
    }

    //void init_metrics() {
    //  for (auto i : _partition_processors) {
    //    for (auto j : i->get_metrics()) {
    //      j->_logged_name = _app_id + "." + _topology_id + ".depth-" + std::to_string(i->depth()) + "." + i->processor_name() + i->record_type_name() + ".partition-" + std::to_string(i->partition()) + "." + j->_simple_name;
    //    }
    //  }

    //  for (auto i : _topic_processors) {
    //    for (auto j : i->get_metrics()) {
    //      j->_logged_name = _app_id + "." + _topology_id + "." + i->processor_name() + i->record_type_name() + "." + j->_simple_name;
    //      //j->_logged_name = _app_id + "." + _topology_id + ".depth-" + std::to_string(i->depth()) + "." + i->processor_name() + "." + j->_simple_name;
    //    }
    //  }
    //}


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

    void output_metrics(std::shared_ptr<kspp::topic_sink<std::string, std::string, kspp::text_codec>> sink) {
      for (auto i : _partition_processors) {
        for (auto j : i->get_metrics())
          sink->produce(std::make_shared<kspp::krecord<std::string, std::string>>(j->name(), std::to_string(j->value())));
      }

      for (auto i : _topic_processors) {
        for (auto j : i->get_metrics()) {
          sink->produce(std::make_shared<kspp::krecord<std::string, std::string>>(j->name(), std::to_string(j->value())));
        }
      }
    }

    //// TBD this should only call most downstream processors??
    //// or should topology call all members??
    //void start(int offset) {
    //  for (auto i : _partition_processors)
    //    i->start(offset);
    //}

    //// TBD this should only call most downstream processors??
    //void flush() {
    //  for (auto i : _partition_processors)
    //    i->flush();
    //}

    /* TBD
    void collect_metrics(merics - sink*) {
    // figure out which is upstream
    // add seq number to processors...
    //
    }
    */

    /**
    creates a kafka sink using default partitioner (hash on key)
    */
    template<class K, class V>
    std::shared_ptr<kspp::topic_sink<K, V, CODEC>> create_kafka_topic_sink(std::string topic) {
      auto p = kspp::kafka_sink<K, V, CODEC>::create(_brokers, topic, _default_codec);
      _topic_processors.push_back(p);
      return p;
    }

    /**
    creates a kafka sink using explicit partitioner
    */
    template<class K, class V>
    std::shared_ptr<kspp::topic_sink<K, V, CODEC>> create_kafka_topic_sink(std::string topic, std::function<uint32_t(const K& key)> partitioner) {
      auto p = kspp::kafka_sink<K, V, CODEC>::create(_brokers, topic, partitioner, _default_codec);
      _topic_processors.push_back(p);
      return p;
    }

    /**
    creates a kafka sink using explicit partition
    */
    template<class K, class V>
    std::shared_ptr<kspp::partition_sink<K, V>> create_kafka_partition_sink(std::string topic) {
      auto p = kspp::kafka_single_partition_sink<K, V, CODEC>::create(_brokers, topic, _partition, _default_codec);
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
    std::shared_ptr<kspp::partition_source<K, V>> create_kafka_source(std::string topic) {
      auto p = std::make_shared<kspp::kafka_source<K, V, CODEC>>(_brokers, topic, _partition, _default_codec);
      _partition_processors.push_back(p);
      return p;
    }

    template<class K, class V>
    std::shared_ptr<kspp::partition_source<K, V>> create_global_kafka_source(std::string topic) {
      auto p = std::make_shared<kspp::kafka_source<K, V, CODEC>>(_brokers, topic, 0, _default_codec);
      _partition_processors.push_back(p);
      return p;
    }

    /*
    template<class K, class V>
    std::vector<std::shared_ptr<kspp::partition_source<K, V>>> create_kafka_sources(std::string topic, size_t nr_of_partitions) {
    std::vector<std::shared_ptr<kspp::partition_source<K, V>>> v;
    for (size_t i = 0; i != nr_of_partitions; ++i)
    v.push_back(create_kafka_source<K, V>(topic, i));
    return v;
    }
    */

    template<class K, class V>
    std::shared_ptr<kspp::kstream_partition<K, V>> create_kstream(std::shared_ptr<kspp::partition_source<K, V>> source) {
      auto p = std::make_shared<kspp::kstream_partition_impl<K, V, CODEC>>(source, get_storage_path(), _default_codec);
      _partition_processors.push_back(p);
      return p;
    }

    template<class K, class V>
    std::shared_ptr<kspp::kstream_partition<K, V>> create_kstream(std::string topic) {
      auto source = create_kafka_source<K, V>(topic);
      return create_kstream<K, V>(source);
      // no adding to _partition_processors
    }

    template<class K, class V>
    std::shared_ptr<kspp::ktable_partition<K, V>> create_ktable(std::shared_ptr<kspp::partition_source<K, V>> source) {
      auto p = std::make_shared<kspp::ktable_partition_impl<K, V, CODEC>>(source, get_storage_path(), _default_codec);
      _partition_processors.push_back(p);
      return p;
    }

    template<class K, class V>
    std::shared_ptr<kspp::ktable_partition<K, V>> create_ktable(std::string topic) {
      auto source = create_kafka_source<K, V>(topic);
      return create_ktable<K, V>(source);
      // no adding to _partition_processors
    }

    template<class K, class V>
    std::shared_ptr<kspp::pipe<K, V>> create_pipe(std::shared_ptr<kspp::partition_source<K, V>> source) {
      auto p = std::make_shared<kspp::pipe<K, V>>(source);
      _partition_processors.push_back(p);
      return p;
    }

    template<class K, class V>
    std::shared_ptr<kspp::pipe<K, V>> create_pipe() {
      auto p = std::make_shared<kspp::pipe<K, V>>(_partition);
      _partition_processors.push_back(p);
      return p;
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

    /*
    template<class K, class V>
    std::vector<std::shared_ptr<kspp::materialized_partition_source<K, V>>> create_count_by_key(std::vector<std::shared_ptr<partition_source<K, void>>>& sources, int64_t punctuate_intervall) {
      std::vector<std::shared_ptr<kspp::materialized_partition_source<K, V>>> res;
      for (auto i : sources)
        res.push_back(create_count_by_key<K, V>(i, punctuate_intervall));
      return res;
    }
    */



    template<class K, class V>
    std::shared_ptr<kspp::partition_stream_sink<K, V>> create_stream_sink(std::shared_ptr<kspp::partition_source<K, V>>source, std::ostream& os) {
      auto p = kspp::partition_stream_sink<K, V>::create(source->partition(), os);
      source->add_sink(p);
      return p;
    }

    // not useful for anything excepts cout or cerr... since everything is bundeled into same stream???
    template<class K, class V>
    std::vector<std::shared_ptr<kspp::partition_stream_sink<K, V>>> create_stream_sinks(std::vector<std::shared_ptr<kspp::partition_source<K, V>>> sources, std::ostream& os) {
      std::vector<std::shared_ptr<kspp::partition_stream_sink<K, V>>> v;
      for (auto s : sources) {
        auto p = create_stream_sink<K, V>(s->partition(), os);
        s->add_sink(p);
        v.push_back(p);
      }
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

    void init() {
      _top_partition_processors.clear();

      for (auto i : _partition_processors) {
        bool upstream_of_something = false;
        for (auto j : _partition_processors) {
          if (j->is_upstream(i.get()))
            upstream_of_something = true;
        }
        if (!upstream_of_something) {
          BOOST_LOG_TRIVIAL(info) << "topology << " << name() << ": adding " << i->name() << " to top";
          _top_partition_processors.push_back(i);
        } else {
          BOOST_LOG_TRIVIAL(info) << "topology << " << name() << ": skipping poll of " << i->name();
        }
      }
      _is_init = true;
    }

    bool eof() {
      for (auto&& i : _top_partition_processors) {
        if (!i->eof())
          return false;
      }
      return true;
    }

    int process_one() {
      // maybe we should check sinks here an return 0 if we need to wait...

      int res = 0;
      for (auto&& i : _top_partition_processors) {
        res += i->process_one();
      }
      // is this nessessary??? are those only sinks??
      for (auto i : _topic_processors)
        res += i->process_one();
      return res;
    }

    void close() {
      for (auto i : _topic_processors)
        i->close();
      for (auto i : _partition_processors)
        i->close();
    }

    void start() {
      if (!_is_init)
        init();
      for (auto&& i : _top_partition_processors)
        i->start();
      //for (auto i : _topic_processors) // those are only sinks??
      //  i->start();
    }

    void start(int offset) {
      if (!_is_init)
        init();
      for (auto&& i : _top_partition_processors)
        i->start(offset);
      //for (auto i : _topic_processors) // those are only sinks??
      //  i->start(offset);
    }

    void flush() {
      for (auto i : _top_partition_processors)
        i->flush();
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
    bool                                              _is_init;
    std::string                                       _app_id;
    std::string                                       _topology_id;
    int32_t                                           _partition;
    std::string                                       _brokers;
    std::shared_ptr<CODEC>                            _default_codec;
    boost::filesystem::path                           _root_path;
    std::vector<std::shared_ptr<partition_processor>> _partition_processors;
    std::vector<std::shared_ptr<topic_processor>>     _topic_processors;
    std::vector<std::shared_ptr<partition_processor>> _top_partition_processors;
  };


  template<class CODEC>
  class topic_topology
  {
  public:
    topic_topology(std::string app_id, std::string topology_id, std::string brokers, boost::filesystem::path root_path, std::shared_ptr<CODEC> default_codec)
      : _app_id(app_id)
      , _is_init(false)
      , _topology_id(topology_id)
      , _brokers(brokers)
      , _default_codec(default_codec)
      , _root_path(root_path) {
      BOOST_LOG_TRIVIAL(info) << "topology created, name:" << name() << ", brokers:" << brokers << " , storage_path:" << root_path;
    }

    ~topic_topology() {
      BOOST_LOG_TRIVIAL(info) << "topology, name:" << name() << " terminated";
      // output stats
    }

    std::string name() const {
      return _app_id + "__" + _topology_id;
    }

    inline std::shared_ptr<CODEC> codec() {
      return _default_codec;
    }

    //void init_metrics() {
    //  for (auto i : _partition_processors) {
    //    for (auto j : i->get_metrics()) {
    //      j->_logged_name = _app_id + "." + _topology_id + ".depth-" + std::to_string(i->depth()) + "." + i->processor_name() + i->record_type_name() + ".partition-" + std::to_string(i->partition()) + "." + j->_simple_name;
    //    }
    //  }

    //  for (auto i : _topic_processors) {
    //    for (auto j : i->get_metrics()) {
    //      j->_logged_name = _app_id + "." + _topology_id + "." + i->processor_name() + i->record_type_name() + "." + j->_simple_name;
    //      //j->_logged_name = _app_id + "." + _topology_id + ".depth-" + std::to_string(i->depth()) + "." + i->processor_name() + "." + j->_simple_name;
    //    }
    //  }
    //}

    void init_metrics() {
      for (auto i : _partition_processors) {
        for (auto j : i->get_metrics()) {
          j->_logged_name = j->_simple_name + ", app_id=" + _app_id + ", topology=" + _topology_id + ", depth=" + std::to_string(i->depth()) + ", processor_type=" + i->processor_name() + ", record_type='" + i->record_type_name() + "', partition=" + std::to_string(i->partition());
          //j->_logged_name = _app_id + "." + _topology_id + ".depth-" + std::to_string(i->depth()) + "." + i->processor_name() + i->record_type_name() + ".partition-" + std::to_string(i->partition()) + "." + j->_simple_name;
        }
      }

      for (auto i : _topic_processors) {
        for (auto j : i->get_metrics()) {
          j->_logged_name = j->_simple_name + ", app_id=" + _app_id + ", topology=" + _topology_id + ", processor_type=" + i->processor_name() + ", record_type='" + i->record_type_name() + "'";
            //j->_logged_name = _app_id + "." + _topology_id + "." + i->processor_name() + i->record_type_name() + "." + j->_simple_name;
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

    void output_metrics(std::shared_ptr<kspp::topic_sink<std::string, std::string, kspp::text_codec>> sink) {
      for (auto i : _partition_processors) {
        for (auto j : i->get_metrics())
          sink->produce(std::make_shared<kspp::krecord<std::string, std::string>>(j->name(), std::to_string(j->value())));
      }

      for (auto i : _topic_processors) {
        for (auto j : i->get_metrics()) {
          sink->produce(std::make_shared<kspp::krecord<std::string, std::string>>(j->name(), std::to_string(j->value())));
        }
      }
    }

    //// TBD this should only call most downstream processors??
    //// or should topology call all members??
    //void start(int offset) {
    //  for (auto i : _partition_processors)
    //    i->start(offset);
    //}
    
    template<class K, class V>
    std::vector<std::shared_ptr<kspp::partition_source<K, V>>> create_kafka_sources(std::string topic, size_t nr_of_partitions) {
      std::vector<std::shared_ptr<kspp::partition_source<K, V>>> result;
      for (size_t i = 0; i != nr_of_partitions; ++i) {
        auto p = std::make_shared<kspp::kafka_source<K, V, CODEC>>(_brokers, topic, i, _default_codec);
        result.push_back(p);
        _partition_processors.push_back(p);
      }
      return result;;
    }

    /**
    creates a kafka sink using default partitioner (hash on key)
    */
    template<class K, class V>
    std::shared_ptr<kspp::topic_sink<K, V, CODEC>> create_kafka_topic_sink(std::string topic) {
      auto p = kspp::kafka_sink<K, V, CODEC>::create(_brokers, topic, _default_codec);
      _topic_processors.push_back(p);
      return p;
    }

    template<class K, class V>
    std::shared_ptr<kspp::topic_sink<K, V, CODEC>> create_kafka_topic_sink(std::vector<std::shared_ptr<kspp::partition_source<K, V>>>& source, std::string topic) {
      auto p = kspp::kafka_sink<K, V, CODEC>::create(_brokers, topic, _default_codec);
      _topic_processors.push_back(p);
      for (auto i : source)
        i->add_sink(p);
      return p;
    }

    /**
    creates a kafka sink using explicit partitioner
    */
    template<class K, class V>
    std::shared_ptr<kspp::topic_sink<K, V, CODEC>> create_kafka_topic_sink(std::string topic, std::function<uint32_t(const K& key)> partitioner) {
      auto p = kspp::kafka_sink<K, V, CODEC>::create(_brokers, topic, partitioner, _default_codec);
      _topic_processors.push_back(p);
      return p;
    }

    template<class K, class V>
    std::shared_ptr<kspp::partition_sink<K, V>> create_global_kafka_sink(std::string topic) {
      auto p = kspp::kafka_single_partition_sink<K, V, CODEC>::create(_brokers, topic, 0, _default_codec);
      _partition_processors.push_back(p);
      return p;
    }

    template<class K, class V>
    std::shared_ptr<kspp::topic_sink<K, V, CODEC>> create_global_kafka_topic_sink(std::vector<std::shared_ptr<kspp::partition_source<K, V>>>& source, std::string topic) {
      auto p = kspp::kafka_single_partition_sink<K, V, CODEC>::create(_brokers, topic, 0, _default_codec);
      _partition_processors.push_back(p);
      for (auto i : source)
        i->add_sink(p);
      return p;
    }

    template<class K, class V>
    std::vector<std::shared_ptr<kspp::ktable_partition<K, V>>> create_ktables(std::vector<std::shared_ptr<kspp::partition_source<K, V>>>& sources) {
      std::vector<std::shared_ptr<kspp::ktable_partition<K, V>>> result;
      for (auto i : sources) {
        auto p = std::make_shared<kspp::ktable_partition_impl<K, V, CODEC>>(i, get_storage_path(), _default_codec);
        result.push_back(p);
        _partition_processors.push_back(p);
      }
      return result;
    }

    template<class K, class V>
    std::vector<std::shared_ptr<kspp::kstream_partition<K, V>>> create_kstreams(std::vector<std::shared_ptr<kspp::partition_source<K, V>>>& sources) {
      std::vector<std::shared_ptr<kspp::kstream_partition<K, V>>> result;
      for (auto i : sources) {
        auto p = std::make_shared<kspp::kstream_partition_impl<K, V, CODEC>>(i, get_storage_path(), _default_codec);
        result.push_back(p);
        _partition_processors.push_back(p);
      }
      return result;
    }

    template<class K, class V>
    std::vector<std::shared_ptr<kspp::pipe<K, V>>> create_pipes(std::vector<std::shared_ptr<kspp::partition_source<K, V>>>& sources) {
      std::vector<std::shared_ptr<kspp::pipe<K, V>>> result;
      for (auto i : sources) {
        auto p = std::make_shared<kspp::pipe<K, V>>(i);
        result.push_back(p);
        _partition_processors.push_back(p);
      }
      return result;
    }

    template<class K, class V>
    std::shared_ptr<kspp::pipe<K, V>> create_global_pipe(std::vector<std::shared_ptr<kspp::partition_source<K, V>>>& sources) {
      auto pipe = std::make_shared<kspp::pipe<K, V>>(0);
      _partition_processors.push_back(pipe);
      for (auto i : sources)
        i->add_sink([pipe](auto e) {
        pipe->produce(e);
      });
      return pipe;
    }


    template<class K, class streamV, class tableV, class R>
    std::vector<std::shared_ptr<kspp::partition_source<K, R>>> create_left_join(
      std::vector<std::shared_ptr<kspp::partition_source<K, streamV>>> source,
      std::vector<std::shared_ptr<kspp::ktable_partition<K, tableV>>> left,
      typename kspp::left_join<K, streamV, tableV, R>::value_joiner value_joiner) {
      std::vector<std::shared_ptr<kspp::partition_source<K, R>>> result;
      assert(source.size() == left.size());
      auto i = source.begin();
      auto j = left.begin();
      auto end = source.end();
      for (; i != end; ++i, ++j) {
        auto p = kspp::left_join<K, streamV, tableV, R>::create(i, j, value_joiner);
        _partition_processors.push_back(p);
        result.push_back(p);
      }
      return result;
    }

    template<class K, class V>
    std::vector<std::shared_ptr<kspp::materialized_partition_source<K, V>>> create_count_by_key(
      std::vector<std::shared_ptr<partition_source<K, void>>>& sources,
      int64_t punctuate_intervall) {
      std::vector<std::shared_ptr<kspp::materialized_partition_source<K, V>>> res;
      for (auto i : sources) {
        auto p = kspp::count_by_key<K, V, CODEC>::create(i, get_storage_path(), punctuate_intervall, _default_codec);
        _partition_processors.push_back(p);
        res.push_back(p);
      }
      return res;
    }

    template<class K, class V>
    void create_stream_sink(const std::vector<std::shared_ptr<kspp::partition_source<K, V>>>& sources, std::ostream& os) {
      for (auto i : sources) {
        auto p = kspp::partition_stream_sink<K, V>::create(i->partition(), os);
        i->add_sink(p);
        _partition_processors.push_back(p);
      }
    }

    template<class K, class V, class PK>
    std::vector<std::shared_ptr<partition_processor>> create_repartition(
      std::vector<std::shared_ptr<kspp::partition_source<K, V>>>& source,
      std::vector<std::shared_ptr<kspp::ktable_partition<K, PK>>>& left,
      std::shared_ptr<topic_sink<K, V, CODEC>> topic_sink) {
      assert(source.size() == left.size());

      std::vector<std::shared_ptr<partition_processor>> result;
      auto begin = source.begin();
      auto lbegin = left.begin();
      auto end = source.end();

      for (; begin != end; ++begin, ++lbegin) {
        auto p = kspp::repartition_by_table<K, V, PK, CODEC>::create(*begin, *lbegin, topic_sink);
        result.push_back(p);
        _partition_processors.push_back(p);
      }
      return result;
    }

    template<class K, class V>
    std::vector<std::shared_ptr<kspp::partition_source<K, V>>> create_filter(
      std::vector<std::shared_ptr<kspp::partition_source<K, V>>> source,
      typename kspp::filter<K, V>::predicate f) {
      std::vector<std::shared_ptr<kspp::partition_source<K, V>>> result;
      for (auto i : source) {
        auto p = kspp::filter<K, V>::create(i, f);
        _partition_processors.push_back(p);
        result.push_back(p);
      }
      return result;;
    }

    template<class SK, class SV, class RK, class RV>
    std::vector<std::shared_ptr<partition_source<RK, RV>>> create_flat_map(
      std::vector<std::shared_ptr<kspp::partition_source<SK, SV>>> source,
      typename kspp::flat_map<SK, SV, RK, RV>::extractor f) {
      std::vector<std::shared_ptr<kspp::partition_source<RK, RV>>> result;
      for (auto i : source) {
        auto p = flat_map<SK, SV, RK, RV>::create(i, f);
        _partition_processors.push_back(p);
        result.push_back(p);
      }
      return result;;
    }

    template<class K, class V>
    std::vector<std::shared_ptr<kspp::partition_source<K, V>>> create_rate_limiter(std::vector<std::shared_ptr<kspp::partition_source<K, V>>> source, int64_t agetime, size_t capacity) {
      std::vector<std::shared_ptr<kspp::partition_source<K, V>>> result;
      for (auto i : source) {
        auto p = rate_limiter<K, V>::create(i, agetime, capacity);
        _partition_processors.push_back(p);
        result.push_back(p);
      }
      return result;;
    }

    template<class K, class V>
    std::shared_ptr<kspp::partition_source<K, V>> create_rate_limiter(std::shared_ptr<kspp::partition_source<K, V>> source, int64_t agetime, size_t capacity) {
      auto result = rate_limiter<K, V>::create(source, agetime, capacity);
      _partition_processors.push_back(result);
      return result;;
    }

    template<class K, class V>
    std::vector<std::shared_ptr<kspp::partition_source<K, V>>> create_thoughput_limiter(std::vector<std::shared_ptr<kspp::partition_source<K, V>>> source, double messages_per_sec) {
      std::vector<std::shared_ptr<kspp::partition_source<K, V>>> result;
      for (auto i : source) {
        auto p = thoughput_limiter<K, V>::create(i, messages_per_sec);
        _partition_processors.push_back(p);
        result.push_back(p);
      }
      return result;;
    }

    /* template<class K, class V>
    std::shared_ptr<kspp::partition_source<K, V>> create_kafka_source(std::string topic) {
      auto p = std::make_shared<kspp::kafka_source<K, V, CODEC>>(_brokers, topic, _partition, _default_codec);
      _partition_processors.push_back(p);
      return p;
    }*/

    void init() {
      _top_partition_processors.clear();

      for (auto i : _partition_processors) {
        bool upstream_of_something = false;
        for (auto j : _partition_processors) {
          if (j->is_upstream(i.get()))
            upstream_of_something = true;
        }
        if (!upstream_of_something) {
          BOOST_LOG_TRIVIAL(info) << "topology << " << name() << ": adding " << i->name() << " to top";
          _top_partition_processors.push_back(i);
        } else {
          BOOST_LOG_TRIVIAL(info) << "topology << " << name() << ": skipping poll of " << i->name();
        }
      }
      /*
       for (std::vector<std::shared_ptr<partition_processor>>::const_iterator i = _partition_processors.begin(); i != _partition_processors.end(); ++i) {
         for (std::vector<std::shared_ptr<partition_processor>>::reverse_iterator j = _top_partition_processors.rbegin(); j != _top_partition_processors.rend(); --j) {
           if (i->is_upstream(j.get())) {
             std::cerr << "-" << j->name();
             _top_partition_processors.erase(j);
           }
         }
       }
       */
      _is_init = true;
    }

    bool eof() {
      for (auto&& i : _top_partition_processors) {
        if (!i->eof())
          return false;
      }
      return true;
    }

    int process_one() {
      int res = 0;
      for (auto&& i : _top_partition_processors) {
        res += i->process_one();
      }

      // is this nessessary???
      for (auto i : _topic_processors)
        res += i->process_one();

      return res;
    }

    void close() {
      for (auto i : _topic_processors)
        i->close();
      for (auto i : _partition_processors)
        i->close();
    }

    void start() {
      if (!_is_init)
        init();
      for (auto&& i : _top_partition_processors)
        i->start();
      for (auto i : _topic_processors) // those are only sinks??
        i->start();
    }

    void start(int offset) {
      if (!_is_init)
        init();
      for (auto&& i : _top_partition_processors)
        i->start(offset);
      //for (auto i : _topic_processors) // those are only sinks??
      //  i->start(offset);
    }

    void flush() {
      for (auto i : _top_partition_processors)
        i->flush();
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
    bool                                              _is_init;
    std::string                                       _topology_id;
    int32_t                                           _partition;
    std::string                                       _brokers;
    std::shared_ptr<CODEC>                            _default_codec;
    boost::filesystem::path                           _root_path;
    std::vector<std::shared_ptr<partition_processor>> _partition_processors;
    std::vector<std::shared_ptr<topic_processor>>     _topic_processors;

    std::vector<std::shared_ptr<partition_processor>> _top_partition_processors;
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

    topology_builder(std::string app_id, std::string brokers = default_kafka_broker(), boost::filesystem::path root_path = default_directory(), std::shared_ptr<CODEC> default_codec = std::make_shared<CODEC>())
      : _app_id(app_id)
      , _next_topology_id(0)
      , _brokers(brokers)
      , _default_codec(default_codec)
      , _root_path(root_path) {
      BOOST_LOG_TRIVIAL(info) << "topology_builder created, app_id:" << app_id << ", kafka_brokers:" << brokers << ", root_path:" << root_path;
    }

    std::shared_ptr<topology<CODEC>> create_topology(std::string id, int32_t partition) {
      return std::make_shared<topology<CODEC>>(_app_id, id, partition, _brokers, _root_path, _default_codec);
    }

    std::shared_ptr<topology<CODEC>> create_topology(int32_t partition) {
      std::string id = "topology-" + std::to_string(_next_topology_id);
      _next_topology_id++;
      return std::make_shared<topology<CODEC>>(_app_id, id, partition, _brokers, _root_path, _default_codec);
    }

    std::shared_ptr<topic_topology<CODEC>> create_topic_topology() {
      std::string id = "topology-" + std::to_string(_next_topology_id);
      _next_topology_id++;
      return std::make_shared<topic_topology<CODEC>>(_app_id, id, _brokers, _root_path, _default_codec);
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
