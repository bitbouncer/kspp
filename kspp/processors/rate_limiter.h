#include <kspp/state_stores/mem_token_bucket_store.h>
#include <chrono>
#pragma once

// this should be a template on storage type 
// ie mem_bucket or rocksdb version
// or virtual ptr to storage to be passed in constructor
// right now this is processing time rate limiting 
// how do we swap betweeen processing and event time??? TBD
namespace kspp {
template<class K, class V>
class rate_limiter : public event_consumer<K, V>, public partition_source<K, V>
{
  public:
  rate_limiter(topology_base& topology, std::shared_ptr<partition_source<K, V>> source, std::chrono::milliseconds agetime, size_t capacity)
    : event_consumer<K, V>()
    , partition_source<K, V>(source.get(), source->partition())
    , _source(source)
    , _token_bucket(std::make_shared<mem_token_bucket_store<K, size_t>>(agetime, capacity))
    , _in_count("in_count")
    , _out_count("out_count")
    , _rejection_count("rejection_count") {
    _source->add_sink([this](auto r) {
      this->_queue.push_back(r);
    });
    this->add_metric(&_lag);
    this->add_metric(&_in_count);
    this->add_metric(&_out_count);
    this->add_metric(&_rejection_count);
  }

  ~rate_limiter() {
    close();
  }

  virtual std::string simple_name() const {
    return "rate_limiter"; 
  }

  virtual void start() {
    _source->start();
  }

  virtual void start(int64_t offset) {
    _source->start(offset);

    if (offset == -2)
      _token_bucket->clear();
  }

  virtual void close() {
    _source->close();
  }

  virtual bool process_one(int64_t tick) {
    _source->process_one(tick);
    bool processed = (this->_queue.size() > 0);
    while (this->_queue.size()) {
      auto ev = this->_queue.front();
      this->_queue.pop_front();
      ++_in_count;
      _lag.add_event_time(tick, ev->event_time());
      // milliseconds_since_epoch for processing time limiter
      // 
      if (_token_bucket->consume(ev->record()->key(), ev->event_time())) { // TBD tick???
        ++_out_count;
        this->send_to_sinks(ev);
      } else {
        ++_rejection_count;
      }
    }
    return processed;
  }

  virtual void commit(bool flush) {
    _source->commit(flush);
  }

  virtual bool eof() const {
    return _source->eof() && (queue_len() == 0);
  }

  virtual size_t queue_len() const {
    return event_consumer<K, V>::queue_len();
  }

  private:
  std::shared_ptr<partition_source<K, V>>            _source;
  std::shared_ptr<mem_token_bucket_store<K, size_t>> _token_bucket;
  metric_lag                                         _lag;
  metric_counter                                     _in_count;
  metric_counter                                     _out_count;
  metric_counter                                     _rejection_count;
};
} // namespace