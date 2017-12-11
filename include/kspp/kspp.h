#include <type_traits>
#include <chrono>
#include <memory>
#include <cstdint>
#include <string>
#include <vector>
#include <mutex>
#include <thread>
#include <chrono>
#include <strstream>
#include <boost/filesystem.hpp>
#include <boost/uuid/uuid.hpp>
#include <boost/algorithm/string/replace.hpp>
#include <glog/logging.h>
#include <kspp/metrics/metrics.h>
#include <kspp/kevent.h>
#include <kspp/type_name.h>
#include <kspp/app_info.h>
#include <kspp/cluster_config.h>
#include <kspp/impl/event_queue.h>
#include <kspp/impl/hash/murmurhash2.h>
#include <kspp/utils/kspp_utils.h>
#include <kspp/event_consumer.h>
#pragma once
namespace kspp {
  class topology;

  enum start_offset_t { OFFSET_BEGINNING=-2, OFFSET_END=-1, OFFSET_STORED=-1000 };

  class processor {
  public:
    virtual ~processor() {}

    /**
     *
     * @return true if all upstream processors are considered at eof
     */
    virtual bool eof() const = 0;

    /**
     *  closes the processor
     */
    virtual void close() = 0;

    const std::vector<metric *> &get_metrics() const {
      return _metrics;
    }


    /**
     *
     * @param name the metric to get
     * @return the current value of the metric
     * note: not fast but useful for debugging
     */
    int64_t get_metric(std::string name) {
      for (auto &&i : _metrics) {
        if (i->_name == name)
          return i->value();
      }
      return -1;
    }

    /**
     *  used for testing (should be removed??)
     */
    virtual void flush() = 0;

    /**
     *
     * @return the type name of the record
     */
    inline std::string record_type_name() const {
      return "[" + key_type_name() + "," + value_type_name() + "]";
    }

    /**
     *
     * @return the type name of the key
     */
    virtual std::string key_type_name() const = 0;

    /**
     *
     * @return the type name of the value
     */
    virtual std::string value_type_name() const = 0;

    /**
     *
     * @return the simple name of the processor
     */
    virtual std::string simple_name() const = 0;

    /**
    * Process an input record -
    */
    virtual bool process_one(int64_t tick) = 0;

    /**
    * returns the inbound queue len
    */
    virtual size_t queue_size() const {
      return 0;
    }

    /**
    * returns the outbound queue len (only sinks has this??? TODO
    */
    virtual size_t outbound_queue_len() const {
      return 0;
    }

    virtual void poll(int timeout) {}

    virtual void punctuate(int64_t timestamp) {}

    /**
    * Do periodic work - will be called infrequently
    * use this to clean out allocated resources that is no longer needed
    */
    virtual void garbage_collect(int64_t tick) {}

    /**
     *
     * @return returns the kafka topic
     */
    virtual std::string topic() const {
      return "";
    }

  protected:
    // must be valid for processor lifetime  (cannot be removed)
    void add_metric(metric *p) {
      _metrics.push_back(p);
    }

    std::vector<metric *> _metrics;
  };


  class partition_processor : public processor {
  public:
    virtual ~partition_processor() {}

    size_t depth() const {
      size_t depth =0;
      for(auto i : upstream_) {
        std::max<size_t>(depth, i->depth()+1);
      }
      return depth;
    }

    virtual bool eof() const {
      for(auto i : upstream_) {
        if (i->eof()==false)
          return false;
      }
      return true;
    }

    virtual void close() {
      for(auto i : upstream_){
        i->close();
      }
      upstream_.clear();
    }

    inline int32_t partition() const {
      return _partition;
    }

    virtual void flush() {
      while (!eof())
        if (!process_one(kspp::milliseconds_since_epoch())) { ;
        }
      for(auto i : upstream_)
        i->flush();
      while (!eof())
        if (!process_one(kspp::milliseconds_since_epoch())) { ;
        }
      punctuate(milliseconds_since_epoch());
    }

    virtual void start(int64_t offset) {
      for(auto i : upstream_)
        i->start(offset);
    }

    virtual void commit(bool flush) = 0;

    bool is_upstream(const partition_processor *node) const {
      // direct children?
      for(auto i : upstream_)
      {
        if (i == node)
          return true;
      }

      // further up?
      for(auto i : upstream_){
        if (i->is_upstream(node))
          return true;
      }
      return false;
    }

  protected:
    partition_processor(partition_processor *upstream, int32_t partition)
        : _partition(partition) {
      if (upstream)
        upstream_.push_back(upstream);
    }

    void add_upstream(partition_processor* p){
      upstream_.push_back(p);
    }

    std::vector<partition_processor*> upstream_;
    const int32_t _partition;
  };

  template<class K, class V>
  class partition_sink : public event_consumer<K, V>, public partition_processor {
  public:
    typedef K key_type;
    typedef V value_type;

    std::string key_type_name() const override {
      return event_consumer<K, V>::key_type_name();
    }

    std::string value_type_name() const override {
      return event_consumer<K, V>::value_type_name();
    }

    size_t queue_size() const override {
      return event_consumer<K, V>::queue_size();
    }

  protected:
    partition_sink(int32_t partition)
        : event_consumer<K, V>(),
          partition_processor(nullptr, partition) {}
  };


  template<class PK, class CODEC>
  inline uint32_t get_partition_hash(const PK &key, std::shared_ptr<CODEC> codec = std::make_shared<CODEC>()) {
    enum { MAX_KEY_SIZE = 1000 };
    uint32_t partition_hash = 0;
    char key_buf[MAX_KEY_SIZE];
    size_t ksize = 0;
    std::strstream s(key_buf, MAX_KEY_SIZE);
    ksize = codec->encode(key, s);
    //partition_hash = djb_hash(key_buf, ksize);
    partition_hash = MurmurHash2(key_buf, (int) ksize, 0x9747b28c);
    return partition_hash;
  }

  template<class PK, class CODEC>
  inline uint32_t
  get_partition(const PK &key, size_t nr_of_partitions, std::shared_ptr<CODEC> codec = std::make_shared<CODEC>()) {
    uint32_t hash = get_partition_hash<PK, CODEC>(key, codec);
    return hash % (uint32_t) nr_of_partitions;
  }

/**
  we need this class to get rid of the codec for templates..
*/
  template<class K, class V>
  class topic_sink : public event_consumer<K, V>, public processor {
  public:
    typedef K key_type;
    typedef V value_type;

    std::string key_type_name() const override {
      return event_consumer<K, V>::key_type_name();
    }

    std::string value_type_name() const override {
      return event_consumer<K, V>::value_type_name();
    }

    size_t queue_size() const override {
      return event_consumer<K, V>::queue_size();
    }

  protected:
  };


  template<class K, class V>
  class partition_source : public partition_processor {
  public:
    using sink_function = typename std::function<void(std::shared_ptr<kevent<K, V>>)>;
    typedef K key_type;
    typedef V value_type;

    partition_source(partition_processor *upstream, int32_t partition)
        : partition_processor(upstream, partition), _out_messages("out_message_count") {
      this->add_metric(&_out_messages);
    }

    std::string key_type_name() const override {
      return type_name<K>::get();
    }

    std::string value_type_name() const override {
      return type_name<V>::get();
    }

    template<class SINK>
    typename std::enable_if<std::is_base_of<kspp::partition_sink<K, V>, SINK>::value, void>::type
    add_sink(SINK *sink) {
      add_sink([sink](auto e) {
        sink->produce(e);
      });
    }

    template<class SINK>
    typename std::enable_if<std::is_base_of<kspp::partition_sink<K, V>, SINK>::value, void>::type
    add_sink(std::shared_ptr<SINK> sink) {
      add_sink([sink](auto e) {
        sink->produce(e);
      });
    }

    template<class SINK>
    typename std::enable_if<std::is_base_of<kspp::topic_sink<K, V>, SINK>::value, void>::type
    add_sink(std::shared_ptr<SINK> sink) {
      add_sink([sink](auto e) {
        sink->produce(e);
      });
    }

    void add_sink(sink_function sink) {
      _sinks.push_back(sink);
    }

  protected:

    virtual void send_to_sinks(std::shared_ptr<kevent<K, V>> p)  {
      if (!p)
        return;
      ++_out_messages;
      for (auto f : _sinks)
        f(p);
    }

    metric_counter _out_messages;
    std::vector<sink_function> _sinks;
  };

  template<class K, class V>
  class kmaterialized_source_iterator_impl {
  public:
    virtual ~kmaterialized_source_iterator_impl() {}

    virtual void next() = 0;

    virtual std::shared_ptr<const krecord<K, V>> item() const = 0;

    virtual bool valid() const = 0;

    virtual bool operator==(const kmaterialized_source_iterator_impl &other) const = 0;

    bool operator!=(const kmaterialized_source_iterator_impl &other) const { return !(*this == other); }
  };

  template<class K, class V>
  class materialized_source : public partition_source<K, V> {
  public:
    class iterator : public std::iterator<
        std::forward_iterator_tag,            // iterator_category
        std::shared_ptr<const krecord<K, V>>, // value_type
        long,                                 // difference_type
        std::shared_ptr<const krecord<K, V>> *,// pointer
        std::shared_ptr<const krecord<K, V>>  // reference
    > {
      std::shared_ptr<kmaterialized_source_iterator_impl<K, V>> _impl;
    public:
      explicit iterator(std::shared_ptr<kmaterialized_source_iterator_impl<K, V>> impl) : _impl(impl) {}

      iterator &operator++() {
        _impl->next();
        return *this;
      }

      iterator operator++(int) {
        iterator retval = *this;
        ++(*this);
        return retval;
      }

      bool operator==(const iterator &other) const { return *_impl == *other._impl; }

      bool operator!=(const iterator &other) const { return !(*this == other); }

      std::shared_ptr<const krecord<K, V>> operator*() const { return _impl->item(); }
    };

    virtual iterator begin() const = 0;

    virtual iterator end() const = 0;

    virtual std::shared_ptr<const krecord<K, V>> get(const K &key) const = 0;

    materialized_source(partition_processor *upstream, int32_t partition)
        : partition_source<K, V>(upstream, partition) {
    }

    virtual boost::filesystem::path get_storage_path(boost::filesystem::path storage_path) {
      boost::filesystem::path p(std::move(storage_path));
      p /= sanitize_filename(this->simple_name() + this->record_type_name() + "#" + std::to_string(this->partition()));
      return p;
    }
  };

  template<class K, class V>
  class merge : public event_consumer<K, V>, public partition_source<K, V> {
  public:
    typedef K key_type;
    typedef V value_type;
    typedef kspp::kevent<K, V> record_type;

    // fix this so source must be descendant from partition source...
    template<class source>
    merge(topology &unused, const std::vector<std::shared_ptr<source>>& upstream, int32_t partition=-1)
        : event_consumer<K, V>()
        , partition_source<K, V>(nullptr, partition) {
      for (auto&& i : upstream) {
        i->add_sink([this](auto r) {
          this->send_to_sinks(r);
        });
      }
    }

    std::string simple_name() const override {
      return "merge";
    }

    bool process_one(int64_t tick) override {
      bool processed = false;
      for (auto i : this->upstream_)
        processed = i->process_one(tick);
      return processed;
    }

    size_t queue_size() const override {
      return event_consumer<K, V>::queue_size();
    }

    void commit(bool force) override {
      for (auto i : this->upstream_)
        i->commit(force);
    }

    // do we have the right hierarchy since those are not overridden and they should????
    int produce(std::shared_ptr<kevent < K, V>> r) {
      this->send_to_sinks(r);
      return 0;
    }

    //int produce(const K &key, const V &value, int64_t ts = kspp::milliseconds_since_epoch()) {
    //  return produce(std::make_shared<kevent<K, V>>(std::make_shared<krecord<K, V>>(key, value, ts)));
    //}
  };

//<null, VALUE>
  template<class V>
  class merge<void, V> : public event_consumer<void, V>, public partition_source<void, V> {
  public:
    typedef void key_type;
    typedef V value_type;
    typedef kspp::kevent<void, V> record_type;

    merge(topology &unused, std::vector<partition_source<void, V>*> upstream, int32_t partition=-1)
        : event_consumer<void, V>()
        , partition_source<void, V>(nullptr, partition) {
      for (auto&& i : upstream) {
        i->add_sink([this](auto r) {
          this->send_to_sinks(r);
        });
      }
    }

    std::string simple_name() const override {
      return "merge";
    }

    bool process_one(int64_t tick) override {
      bool processed = false;
      for (auto i : this->upstream_)
        processed = i->process_one(tick);
      return processed;
    }

    size_t queue_size() const override {
      return event_consumer<void, V>::queue_size();
    }

    void commit(bool force) override {
      for (auto i : this->upstream_)
        i->commit(force);
    }

    int produce(std::shared_ptr<kevent < void, V>> r) {
      this->send_to_sinks(r);
      return 0;
    }

    int produce(const V &value) {
      return produce(std::make_shared<kevent<void, V>>(std::make_shared<krecord<void, V>>(value)));
    }

  };

  template<class K>
  class merge<K, void> : public event_consumer<K, void>, public partition_source<K, void> {
  public:
    typedef K key_type;
    typedef void value_type;
    typedef kspp::kevent<K, void> record_type;

    merge(topology &unused, std::vector<partition_source<K, void>*> upstream, int32_t partition=-1)
        : event_consumer<K, void>()
        , partition_source<K, void>(nullptr, partition) {
      for (auto&& i : upstream) {
        i->add_sink([this](auto r) {
          this->send_to_sinks(r);
        });
      }
    }

    std::string simple_name() const override {
      return "merge";
    }

    bool process_one(int64_t tick) override {
      bool processed = false;
      for (auto i : this->upstream_)
        processed = i->process_one(tick);
      return processed;
    }

    size_t queue_size() const override {
      return event_consumer<K, void>::queue_size();
    }

    void commit(bool force) override {
      for (auto i : this->upstream_)
        i->commit(force);
    }

    int produce(std::shared_ptr<kevent < K, void>> r) {
      this->send_to_sinks(r);
      return 0;
    }

    int produce(const K &key) {
      return produce(std::make_shared<kevent<K, void>>(std::make_shared<krecord<K, void>>(key)));
    }
  };


} // namespace