#include <kspp/kspp.h>
#pragma once

namespace kspp {
  template<class K, class V>
  class mem_stream_source : public event_consumer<K, V>, public partition_source<K, V> {
    static constexpr const char* PROCESSOR_NAME = "mem_stream_source";
  public:
    typedef K key_type;
    typedef V value_type;
    typedef kspp::kevent<K, V> record_type;

    mem_stream_source(std::shared_ptr<cluster_config> config, int32_t partition)
        : event_consumer<K, V>()
        , partition_source<K, V>(nullptr, partition) {
      this->add_metrics_label(KSPP_PROCESSOR_TYPE_TAG, "generic_stream");
      this->add_metrics_label(KSPP_PARTITION_TAG, std::to_string(partition));
    }

    mem_stream_source(std::shared_ptr<cluster_config> config, std::shared_ptr<kspp::partition_source<K, V>> upstream)
        : event_consumer<K, V>()
        , partition_source<K, V>(upstream.get(), upstream->partition()) {
      upstream->add_sink([this](auto e) {
        this->_queue.push_back(e);
      });
      this->add_metrics_label(KSPP_PROCESSOR_TYPE_TAG, "generic_stream");
      this->add_metrics_label(KSPP_PARTITION_TAG, std::to_string(upstream->partition()));
    }

    mem_stream_source(std::shared_ptr<cluster_config> config, std::vector<std::shared_ptr<kspp::partition_source<K, V>>> upstream, int32_t partition)
        : event_consumer<K, V>()
        , partition_source<K, V>(nullptr, partition) {
      for (auto i : upstream) {
        this->add_upstream(i.get());
        i->add_sink([this](auto e) {
          this->_queue.push_back(e);
        });
      }
      this->add_metrics_label(KSPP_PROCESSOR_TYPE_TAG, "generic_stream");
      this->add_metrics_label(KSPP_PARTITION_TAG, std::to_string(partition));
    }

    std::string log_name() const override {
      return PROCESSOR_NAME;
    }

    size_t process(int64_t tick) override {
      for (auto i : this->upstream_)
        i->process(tick);

      size_t processed=0;
      //forward up this timestamp
      while (this->_queue.next_event_time()<=tick){
        auto p = this->_queue.pop_front_and_get();
        this->send_to_sinks(p);
        ++(this->_processed_count);
        this->_lag.add_event_time(kspp::milliseconds_since_epoch(), p->event_time()); // move outside loop
        ++processed;
      }
      return processed;
    }


    size_t queue_size() const override {
      return event_consumer<K, V>::queue_size();
    }

    int64_t next_event_time() const override {
      return event_consumer<K, V>::next_event_time();
    }


    void commit(bool force) override {
      for (auto i : this->upstream_)
        i->commit(force);
    }
  };

//<null, VALUE>
  template<class V>
  class mem_stream_source<void, V> : public event_consumer<void, V>, public partition_source<void, V> {
    static constexpr const char* PROCESSOR_NAME = "mem_stream_source";
  public:
    typedef void key_type;
    typedef V value_type;
    typedef kspp::kevent<void, V> record_type;

    mem_stream_source(std::shared_ptr<cluster_config> config, int32_t partition)
        : event_consumer<void, V>()
        , partition_source<void, V>(nullptr, partition) {
      this->add_metrics_label(KSPP_PROCESSOR_TYPE_TAG, "generic_stream");
      this->add_metrics_label(KSPP_PARTITION_TAG, std::to_string(partition));
    }

    mem_stream_source(std::shared_ptr<cluster_config> config, std::shared_ptr<kspp::partition_source<void, V>> upstream)
        : event_consumer<void, V>(), partition_source<void, V>(upstream.get(), upstream->partition()) {
      if (upstream)
        upstream->add_sink([this](auto e) {
          this->_queue.push_back(e);
        });
      this->add_metrics_label(KSPP_PROCESSOR_TYPE_TAG, "generic_stream");
      this->add_metrics_label(KSPP_PARTITION_TAG, std::to_string(upstream->partition()));
    }

    mem_stream_source(std::shared_ptr<cluster_config> config, std::vector<std::shared_ptr<kspp::partition_source<void, V>>> upstream, int32_t partition)
        : event_consumer<void, V>()
        , partition_source<void, V>(nullptr, partition) {
      for (auto i : upstream) {
        this->add_upstream(i.get());
        i->add_sink([this](auto e) {
          this->_queue.push_back(e);
        });
      }
      this->add_metrics_label(KSPP_PROCESSOR_TYPE_TAG, "generic_stream");
      this->add_metrics_label(KSPP_PARTITION_TAG, std::to_string(partition));
    }

    std::string log_name() const override {
      return PROCESSOR_NAME;
    }

    size_t process(int64_t tick) override {
      for (auto i : this->upstream_)
        i->process(tick);

      size_t processed=0;

      //forward up this timestamp
      while (this->_queue.next_event_time()<=tick){
        auto p = this->_queue.pop_front_and_get();
        this->send_to_sinks(p);
        ++(this->_processed_count);
        this->_lag.add_event_time(kspp::milliseconds_since_epoch(), p->event_time()); // move outside loop
        ++processed;
      }
      return processed;
    }

    size_t queue_size() const override {
      return event_consumer<void, V>::queue_size();
    }

    int64_t next_event_time() const override {
      return event_consumer<void, V>::next_event_time();
    }


    void commit(bool force) override {
      for (auto i : this->upstream_)
        i->commit(force);
    }
  };

  template<class K>
  class mem_stream_source<K, void> : public event_consumer<K, void>, public partition_source<K, void> {
    static constexpr const char* PROCESSOR_NAME = "mem_stream_source";
  public:
    typedef K key_type;
    typedef void value_type;
    typedef kspp::kevent<K, void> record_type;

    mem_stream_source(std::shared_ptr<cluster_config> config, int32_t partition)
        : event_consumer<K, void>()
        , partition_source<K, void>(nullptr, partition) {
      this->add_metrics_label(KSPP_PROCESSOR_TYPE_TAG, "generic_stream");
      this->add_metrics_label(KSPP_PARTITION_TAG, std::to_string(partition));
    }

    mem_stream_source(std::shared_ptr<cluster_config> config, std::shared_ptr<kspp::partition_source<K, void>> upstream)
        : event_consumer<K, void>()
        , partition_source<K, void>(upstream.get(), upstream->partition()) {
      if (upstream)
        upstream->add_sink([this](std::shared_ptr<kevent<K, void>> e) {
          this->_queue.push_back(e);
        });
      this->add_metrics_label(KSPP_PROCESSOR_TYPE_TAG, "generic_stream");
      this->add_metrics_label(KSPP_PARTITION_TAG, std::to_string(upstream->partition()));
    }

    mem_stream_source(std::shared_ptr<cluster_config> config, std::vector<std::shared_ptr<kspp::partition_source<K, void>>> upstream, int32_t partition)
        : event_consumer<K, void>()
        , partition_source<K, void>(nullptr, partition) {
      this->add_metrics_label(KSPP_PROCESSOR_TYPE_TAG, "generic_stream");
      for (auto i : upstream) {
        this->add_upstream(i.get());
        i->add_sink([this](auto e) {
          this->_queue.push_back(e);
        });
      }
    }

    std::string log_name() const override {
      return PROCESSOR_NAME;
    }

    size_t process(int64_t tick) override {
      for (auto i : this->upstream_)
        i->process(tick);

      size_t processed=0;
      //forward up this timestamp
      while (this->_queue.next_event_time()<=tick){
        auto p = this->_queue.pop_front_and_get();
        this->send_to_sinks(p);
        ++(this->_processed_count);
        this->_lag.add_event_time(kspp::milliseconds_since_epoch(), p->event_time()); // move outside loop
        ++processed;
      }
      return processed;
    }

    size_t queue_size() const override {
      return event_consumer<K, void>::queue_size();
    }

    int64_t next_event_time() const override {
      return event_consumer<K, void>::next_event_time();
    }

    void commit(bool force) override {
      for (auto i : this->upstream_)
        i->commit(force);
    }
  };
}