#include <kspp/state_stores/token_bucket.h>
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
  rate_limiter(topology_base& topology, std::shared_ptr<partition_source<K, V>> source, int64_t agetime, size_t capacity)
    : partition_source<K, V>(source.get(), source->partition())
    , _source(source)
    , _token_bucket(std::make_shared<mem_token_bucket<K>>(agetime, capacity)) {
    _source->add_sink([this](auto r) {
      _queue.push_back(r);
    });
    this->add_metric(&_lag);
  }

  ~rate_limiter() {
    close();
  }

  virtual std::string processor_name() const { return "rate_limiter"; }

  /*static std::vector<std::shared_ptr<partition_source<K, V>>> create(std::vector<std::shared_ptr<partition_source<K, V>>>& streams, int64_t agetime, size_t capacity) {
    std::vector<std::shared_ptr<partition_source<K, V>>> res;
    for (auto i : streams)
      res.push_back(std::make_shared<rate_limiter<K, V>>(i, agetime, capacity));
    return res;
  }

  static std::shared_ptr<partition_source<K, V>> create(std::shared_ptr<partition_source<K, V>> source, int64_t agetime, size_t capacity) {
    return std::make_shared<rate_limiter<K, V>>(source, agetime, capacity);
  }*/

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

  virtual bool process_one() {
    _source->process_one();
    bool processed = (_queue.size() > 0);
    while (_queue.size()) {
      auto r = _queue.front();
      _queue.pop_front();
      // milliseconds_since_epoch for processing time limiter
      // 
      if (_token_bucket->consume(r->key, r->event_time))
        this->send_to_sinks(r);
    }
    return processed;
  }

  virtual void commit() {
    _source->commit();
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
  std::shared_ptr<token_bucket<K>>           _token_bucket;
  metric_lag                                 _lag;
};

template<class K, class V>
class thoughput_limiter : public partition_source<K, V>
{
  public:
  thoughput_limiter(topology_base& topology, std::shared_ptr<partition_source<K, V>> source, double messages_per_sec)
    : partition_source<K, V>(source.get(), source->partition())
    , _source(source)
    , _token_bucket(std::make_shared<mem_token_bucket<int>>((int64_t) (1000.0/messages_per_sec), 1)) {
    _source->add_sink([this](auto r) {
      _queue.push_back(r);
    });
  }

  ~thoughput_limiter() {
    close();
  }

 /* static std::vector<std::shared_ptr<partition_source<K, V>>> create(std::vector<std::shared_ptr<partition_source<K, V>>>& streams, double messages_per_sec) {
    std::vector<std::shared_ptr<partition_source<K, V>>> res;
    for (auto i : streams)
      res.push_back(std::make_shared<thoughput_limiter<K, V>>(i, messages_per_sec));
    return res;
  }

  static std::shared_ptr<partition_source<K, V>> create(std::shared_ptr<partition_source<K, V>> source, double messages_per_sec) {
    return std::make_shared<thoughput_limiter<K, V>>(source, messages_per_sec);
  }*/

  std::string name() const {
    return _source->name() + "-thoughput_limiter";
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

  virtual bool process_one() {
    if (_queue.size() == 0)
      _source->process_one();

    if (_queue.size()) {
      auto r = _queue.front();
      _lag.add_event_time(r->event_time);
      if (_token_bucket->consume(0, milliseconds_since_epoch())) {
        _queue.pop_front();
        this->send_to_sinks(r);
        return true;
      }
    }
    return false;
  }

  virtual void commit() {
    _source->commit();
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
  std::shared_ptr<token_bucket<int>>         _token_bucket;
  metric_lag                                 _lag;
};

} // namespace