#include <functional>
#include <deque>
#include <kspp/kspp.h>

#pragma once

namespace kspp {
  template<class K, class V>
  class visitor : public partition_sink<K, V> {
    static constexpr const char* PROCESSOR_NAME = "visitor";
  public:
    typedef std::function<void(std::shared_ptr<const krecord <K, V>> record)> extractor;

    visitor(std::shared_ptr<cluster_config> config, std::shared_ptr<partition_source<K, V>> source,  extractor f)
    : partition_sink<K, V>(source->partition())
    , _source(source)
    , _extractor(f) {
      _source->add_sink([this](auto r) { this->_queue.push_back(r); });
      this->add_metrics_label(KSPP_PROCESSOR_TYPE_TAG, PROCESSOR_NAME);
      this->add_metrics_label(KSPP_PARTITION_TAG, std::to_string(source->partition()));
    }

    ~visitor() {
      close();
    }

    std::string log_name() const override {
      return PROCESSOR_NAME;
    }

    void start(int64_t offset) override {
      _source->start(offset);
    }

    void close() override {
      _source->close();
    }

    size_t process(int64_t tick) override {
      _source->process(tick);
      size_t processed=0;
      while (this->_queue.next_event_time()<=tick){
        auto trans = this->_queue.pop_and_get();
        ++processed;
        this->_lag.add_event_time(tick, trans->event_time());
        ++(this->_processed_count);
        _extractor(trans->record());
      }
      return processed;
    }

    void commit(bool flush) override {
      _source->commit(flush);
    }

    bool eof() const override {
      return queue_size() == 0 && _source->eof();
    }

    size_t queue_size() const override {
      return event_consumer<K, V>::queue_size();
    }

    int64_t next_event_time() const override {
      return event_consumer<K, V>::next_event_time();
    }

  private:
    std::shared_ptr<partition_source <K, V>> _source;
    extractor _extractor;
  };
}
