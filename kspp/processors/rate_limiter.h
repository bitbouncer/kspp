#include <kspp/impl/state_stores/mem_token_bucket_store.h>
#include <chrono>
#pragma once

// this should be a template on storage type 
// ie mem_bucket or rocksdb version
// or virtual ptr to storage to be passed in constructor
// right now this is processing time rate limiting 
// how do we swap betweeen processing and event time??? TBD
namespace kspp {
template<class K, class V>
class rate_limiter : public partition_source<K, V>
{
  public:
  rate_limiter(topology_base& topology, std::shared_ptr<partition_source<K, V>> source, std::chrono::milliseconds agetime, size_t capacity)
    : partition_source<K, V>(source.get(), source->partition())
    , _source(source)
    , _token_bucket(std::make_shared<mem_token_bucket_store<K>>(agetime, capacity))
    , _in_count("in_count")
    , _out_count("out_count")
    , _rejection_count("rejection_count") {
    _source->add_sink([this](auto r) {
      _queue.push_back(r);
    });
    this->add_metric(&_lag);
    this->add_metric(&_in_count);
    this->add_metric(&_out_count);
    this->add_metric(&_rejection_count);
  }

  ~rate_limiter() {
    close();
  }

  virtual std::string processor_name() const { 
    return "rate_limiter"; 
  }

  std::string name() const {
    return _source->name() + "-rate_limiter";
  }

  virtual void start() {
    _source->start();
  }

  virtual void start(int64_t offset) {
    _source->start(offset);

    if (offset == -2)
      _token_bucket->erase();
  }

  virtual void close() {
    _source->close();
  }

  virtual bool process_one(int64_t tick) {
    _source->process_one(tick);
    bool processed = (_queue.size() > 0);
    while (_queue.size()) {
      auto r = _queue.front();
      _queue.pop_front();
      ++_in_count;
      _lag.add_event_time(tick, r->event_time);
      // milliseconds_since_epoch for processing time limiter
      // 
      if (_token_bucket->consume(r->key, r->event_time)) { // TBD tick???
        ++_out_count;
        this->send_to_sinks(r);
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
    return _source->eof() && (_queue.size() == 0);
  }

  virtual size_t queue_len() {
    return _queue.size();
  }

  private:
  std::shared_ptr<partition_source<K, V>>    _source;
  std::deque<std::shared_ptr<krecord<K, V>>> _queue;
  std::shared_ptr<token_bucket_store<K>>     _token_bucket;
  metric_lag                                 _lag;
  metric_counter                             _in_count;
  metric_counter                             _out_count;
  metric_counter                             _rejection_count;
};
} // namespace