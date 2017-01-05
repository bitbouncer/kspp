#include <type_traits>
#include <chrono>
#include <memory>
#include <cstdint>
#include <string>
#include <vector>

#pragma once
namespace csi {
  inline int64_t milliseconds_since_epoch() {
    return std::chrono::duration_cast<std::chrono::milliseconds>
      (std::chrono::system_clock::now().time_since_epoch()).count();
  }

  template<class K, class V>
  struct krecord
  {
    krecord() : event_time(-1), offset(-1) {}
    krecord(const K& k) : event_time(milliseconds_since_epoch()), offset(-1), key(k) {}
    krecord(const K& k, const V& v) : event_time(milliseconds_since_epoch()), offset(-1), key(k), value(std::make_shared<V>(v)) {}
    krecord(const K& k, std::shared_ptr<V> v) : event_time(milliseconds_since_epoch()), offset(-1), key(k), value(v) {}
    krecord(const K& k, std::shared_ptr<V> v, int64_t ts) : event_time(ts), offset(-1), key(k), value(v) {}

    K                  key;
    std::shared_ptr<V> value;
    int64_t            event_time;
    int64_t            offset;
  };

  template<class V>
  struct krecord<void, V>
  {
    krecord(const V& v) : event_time(milliseconds_since_epoch()), offset(-1), value(std::make_shared<V>(v)) {}
    krecord(std::shared_ptr<V> v) : event_time(milliseconds_since_epoch()), offset(-1), value(v) {}
    krecord(std::shared_ptr<V> v, int64_t ts) : event_time(ts), offset(-1), value(v) {}
    std::shared_ptr<V> value;
    int64_t            event_time;
    int64_t            offset;
  };

  template<class K>
  struct krecord<K, void>
  {
    krecord() : event_time(-1), offset(-1) {}
    krecord(const K& k) : event_time(milliseconds_since_epoch()), offset(-1), key(k) {}
    krecord(const K& k, int64_t ts) : event_time(ts), offset(-1), key(k) {}
    K                  key;
    int64_t            event_time;
    int64_t            offset;
  };

  class processor_context
  {
  public:
    processor_context() {
    }
    /**
    * Returns the application id
    *
    * @return the application id
    */
    std::string application_id() { return "xxx";  }
    /**
    * Returns the state directory for the partition.
    *
    * @return the state directory
    */
    std::string state_dir() { return "";  }
    /**
    * Returns the task id
    *
    * @return the task id
    */
    int32_t task_id() { return 0;  }
    /**
    * Schedules a periodic operation for processors. A processor may call this method during
    * {@link Processor#init(ProcessorContext) initialization} to
    * schedule a periodic call called a punctuation to {@link Processor#punctuate(long)}.
    *
    * @param interval the time interval between punctuations
    */
    void schedule(int32_t interval) {
    }
    /**
    * Requests a commit
    */
    void commit() {

    }
    /**
    * Returns the partition id of the current input record; could be -1 if it is not
    * available (for example, if this method is invoked from the punctuate call)
    *
    * @return the partition id
    */
    int32_t partition() { return -1; }
    /**
    * Returns the offset of the current input record; could be -1 if it is not
    * available (for example, if this method is invoked from the punctuate call)
    *
    * @return the offset
    */
    int64_t offset() { return -1; }
    /**
    * Returns the current timestamp.
    *
    * If it is triggered while processing a record streamed from the source processor, timestamp is defined as the timestamp of the current input record; the timestamp is extracted from
    * {@link org.apache.kafka.clients.consumer.ConsumerRecord ConsumerRecord} by {@link TimestampExtractor}.
    *
    * If it is triggered while processing a record generated not from the source processor (for example,
    * if this method is invoked from the punctuate call), timestamp is defined as the current
    * task's stream time, which is defined as the smallest among all its input stream partition timestamps.
    *
    * @return the timestamp
    */
    int64_t timestamp() { return 0;  }
  };

  class topic_processor
  {
  public:
    virtual ~topic_processor() {}
    virtual void init(processor_context*) {}
    virtual std::string name() const = 0;

    virtual void poll(int timeout) {}

    /**
    * Process an input record
    */
    virtual bool process_one() = 0;

    virtual void punctuate(int64_t timestamp) {}
    virtual void close() = 0;
  protected:
  };

  class partition_processor
  {
  public:
    virtual ~partition_processor() {}
    virtual void init(processor_context*) {}

    virtual std::string name() const = 0;

    /**
    * Process an input record
    */
    virtual bool process_one() = 0;
    virtual void poll(int timeout) {}
    virtual void punctuate(int64_t timestamp) {}
    virtual void close() = 0;
    inline uint32_t partition() const {
      return (uint32_t) _partition;
    }
  protected:
    partition_processor(size_t partition)
      : _partition(partition) {}
    const size_t _partition;
  };


  class topoplogy
  {
  public:
    topoplogy() {
    }

    void add(std::shared_ptr<partition_processor> p) {
      _partition_processors.push_back(p);
    }

    void add(std::shared_ptr<topic_processor> p) {
      _topic_processors.push_back(p);
    }

    bool start(){}
    bool process_init_eof () { return true; }
    bool process_init() { return false;  }
    bool eof() { return true; }
    bool process() { return false;  }
  protected:
    std::vector<std::shared_ptr<partition_processor>> _partition_processors;
    std::vector<std::shared_ptr<topic_processor>>     _topic_processors;
  };

  template<class K, class V>
  class partition_sink : public partition_processor
  {
  public:
    typedef K key_type;
    typedef V value_type;
    typedef csi::krecord<K, V> record_type;

    virtual int produce(std::shared_ptr<krecord<K, V>> r) = 0;
    inline int produce(const K& key, const V& value) {
      return produce(std::make_shared<krecord<K, V>>(key, value));
    }

    virtual size_t queue_len() = 0;
  protected:
    partition_sink(size_t partition)
      : partition_processor(partition) {}
  };

  // specialisation for void key
  template<class V>
  class partition_sink<void, V> : public partition_processor
  {
    public:
    typedef void key_type;
    typedef V value_type;
    typedef csi::krecord<void, V> record_type;

    virtual int produce(std::shared_ptr<krecord<void, V>> r) = 0;
    inline int produce(const V& value) {
      return produce(std::make_shared<krecord<void, V>>(value));
    }

    virtual size_t queue_len() = 0;
    protected:
    partition_sink(size_t partition)
      : partition_processor(partition) {}
  };


   inline uint32_t djb_hash(const char *str, size_t len) {
    uint32_t hash = 5381;
    for (size_t i = 0; i < len; i++)
      hash = ((hash << 5) + hash) + str[i];
    return hash;
  }

  template<class PK, class CODEC>
  inline uint32_t get_partition_hash(const PK& key, std::shared_ptr<CODEC> codec = std::make_shared<CODEC>()) {
    enum { MAX_KEY_SIZE = 1000 };
    uint32_t partition_hash = 0;
    char key_buf[MAX_KEY_SIZE];
    size_t ksize = 0;
    std::strstream s(key_buf, MAX_KEY_SIZE);
    ksize = codec->encode(key, s);
    partition_hash = djb_hash(key_buf, ksize);
    return partition_hash;
  }
  
  template<class PK, class CODEC>
  inline uint32_t get_partition(const PK& key, size_t nr_of_partitions, std::shared_ptr<CODEC> codec = std::make_shared<CODEC>()) {
    auto hash = get_partition_hash <PK, CODEC>(key, codec);
    return hash % nr_of_partitions;
  }

  template<class K, class V, class CODEC>
  class topic_sink : public topic_processor
  {
  public:
    typedef K key_type;
    typedef V value_type;
    typedef csi::krecord<K, V> record_type;

    virtual int    produce(std::shared_ptr<krecord<K, V>> r) = 0;
    virtual size_t queue_len() = 0;
    virtual int produce(uint32_t partition_hash, std::shared_ptr<krecord<K, V>> r) = 0;
    inline  int produce(uint32_t partition_hash, const K& key, const V& value) {
      return produce(partition_hash, std::make_shared<krecord<K, V>>(key, value));
    }

    inline std::shared_ptr<CODEC> codec() {
      return _codec;
    }
  protected:
    topic_sink(std::shared_ptr<CODEC> codec)
      :_codec(codec) {}

    std::shared_ptr<CODEC> _codec;
  };

  // specialisation for void key
  template<class V, class CODEC>
  class topic_sink<void, V, CODEC> : public topic_processor
  {
    public:
    typedef void key_type;
    typedef V value_type;
    typedef csi::krecord<void, V> record_type;

    virtual int produce(std::shared_ptr<krecord<void, V>> r) = 0;
    virtual size_t queue_len() = 0;
    virtual int produce(uint32_t partition_hash, std::shared_ptr<krecord<void, V>> r) = 0;
    inline  int produce(uint32_t partition_hash, const V& value) {
      return produce(partition_hash, std::make_shared<krecord<void, V>>(value));
    }

    inline std::shared_ptr<CODEC> codec() {
      return _codec;
    }

    protected:
    topic_sink(std::shared_ptr<CODEC> codec)
      :_codec(codec) {}

    std::shared_ptr<CODEC> _codec;
  };



  template<class K, class V>
  class partition_source : public partition_processor
  {
  public:
    using sink_function = typename std::function<void(std::shared_ptr<krecord<K, V>>)>;

    partition_source(uint32_t partition)
      :partition_processor(partition) {}

    virtual void add_sink(std::shared_ptr<partition_sink<K, V>> sink) {
      add_sink([sink](auto e) {
        sink->produce(e);
      });
    }

    virtual void add_sink(sink_function sink) {
      _sinks.push_back(sink);
    }

    virtual bool eof() const = 0;
    virtual void start() {}
    virtual void start(int64_t offset) {}
    virtual void commit() {}
    virtual void flush_offset() {}
  protected:

    virtual void send_to_sinks(std::shared_ptr<krecord<K, V>> p) {
      for (auto f : _sinks)
        f(p);
    }

    std::vector<sink_function> _sinks;
  };

  template<class K, class V>
  class kmaterialized_source_iterator_impl
  {
  public:
    virtual ~kmaterialized_source_iterator_impl() {}
    virtual void next() = 0;
    virtual std::shared_ptr<krecord<K, V>> item() const = 0;
    virtual bool valid() const = 0;
    virtual bool operator==(const kmaterialized_source_iterator_impl& other) const = 0;
    bool operator!=(const kmaterialized_source_iterator_impl& other) const { return !(*this == other); }
  };

  template<class K, class V>
  class materialized_partition_source : public partition_source<K, V>
  {
  public:
    class iterator : public std::iterator <
      std::forward_iterator_tag,      // iterator_category
      std::shared_ptr<krecord<K, V>>, // value_type
      long,                           // difference_type
      std::shared_ptr<krecord<K, V>>*,// pointer
      std::shared_ptr<krecord<K, V>>  // reference
    >
    {
      std::shared_ptr<kmaterialized_source_iterator_impl<K, V>> _impl;
    public:
      explicit iterator(std::shared_ptr<kmaterialized_source_iterator_impl<K, V>> impl) : _impl(impl) {}
      iterator& operator++() { _impl->next(); return *this; }
      iterator operator++(int) { iterator retval = *this; ++(*this); return retval; }
      bool operator==(const iterator& other) const { return *_impl == *other._impl; }
      bool operator!=(const iterator& other) const { return !(*this == other); }
      std::shared_ptr<krecord<K, V>> operator*() const { return _impl->item(); }
    };
    virtual iterator begin() = 0;
    virtual iterator end() = 0;
    virtual std::shared_ptr<krecord<K, V>> get(const K& key) = 0;

    materialized_partition_source(uint32_t partition)
      :partition_source(partition) {}
  };

  template<class K, class V>
  class kstream_partition : public partition_source<K, V>
  {
  public:
    kstream_partition(uint32_t partition)
      : partition_source(partition) {}
  };

  template<class K, class V>
  class ktable_partition : public materialized_partition_source<K, V>
  {
  public:
    ktable_partition(uint32_t partition)
      : materialized_partition_source(partition) {}
  };

  template<class K, class V>
  std::shared_ptr<krecord<K, V>> create_krecord(const K& k, const V& v) {
    return std::make_shared<krecord<K, V>>(k, std::make_shared<V>(v));
  }

  template<class K, class V>
  std::shared_ptr<krecord<K, V>> create_krecord(const K& k) {
    return std::make_shared<krecord<K, V>>(k);
  }

  template<class K, class V>
  void process_one(const std::vector<std::shared_ptr<partition_source<K, V>>>& sources) {
    for (auto i : sources) {
      i->process_one();
    }
  }

  template<class K, class V>
  bool eof(std::vector<std::shared_ptr<partition_source<K, V>>>& sources) {
    for (auto i : sources) {
      if (!i->eof())
        return false;
    }
    return true;
  }

  template<class K, class V>
  bool eof(std::vector<std::shared_ptr<materialized_partition_source<K, V>>>& sources) {
    for (auto i : sources) {
      if (!i->eof())
        return false;
    }
    return true;
  }

  template<class KSINK>
  int produce(KSINK& sink, const typename KSINK::key_type& key) {
    return sink.produce(std::make_shared<KSINK::record_type>(key));
  }

  template<class KSINK>
  int produce(KSINK& sink, const typename KSINK::key_type& key, const typename KSINK::value_type& value) {
    return sink.produce(std::make_shared<KSINK::record_type>(key, value));
  }

  template<class KSINK>
  int produce(KSINK& sink, const typename KSINK::value_type& value) {
    return sink.produce(std::make_shared<KSINK::record_type>(void, value));
  }

  // TBD bestämm vilket api som är bäst...

  template<class K, class V>
  int produce(partition_sink<K, V>& sink, const K& key, const V& val) {
    return sink.produce(std::move<>(create_krecord<K, V>(key, val)));
  }

  template<class K, class V>
  int produce(partition_sink<void, V>& sink, const V& val) {
    return sink.produce(std::move<>(std::make_shared<krecord<void, V>>(val)));
  }

  template<class K, class V>
  int produce(partition_sink<K, V>& sink, const K& key) {
    return sink.produce(std::move<>(create_krecord<K, V>(key)));
  }
}; // namespace