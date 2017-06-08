#pragma once
namespace kspp {
  template<class K, class V>
  class filter : public event_consumer<K, V>, public partition_source<K, V>
  {
  public:
    typedef std::function<bool(std::shared_ptr<const krecord<K, V>> record)> predicate; // return true to keep

    filter(topology_base& topology, std::shared_ptr<partition_source<K, V>> source, predicate f)
      : event_consumer<K, V>()
      , partition_source<K, V>(source.get(), source->partition())
      , _source(source)
      , _predicate(f)
      , _predicate_false("predicate_false") {
      _source->add_sink([this](auto r) {
        _queue.push_back(r);
      });
      this->add_metric(&_lag);
      this->add_metric(&_predicate_false);
    }

    ~filter() {
      close();
    }

    virtual std::string simple_name() const {
      return "filter"; 
    }
 
    virtual void start() {
      _source->start();
    }

    virtual void start(int64_t offset) {
      _source->start(offset);
    }

    virtual void close() {
      _source->close();
    }

    virtual bool process_one(int64_t tick) {
      if (_queue.size()==0)
        _source->process_one(tick);
      bool processed = (_queue.size() > 0);
      while (_queue.size()) {
        auto ev = _queue.front();
        _queue.pop_front();
        _lag.add_event_time(tick, ev->event_time());
        if (_predicate(ev->record())) {
          this->send_to_sinks(ev);
        } else {
          ++_predicate_false;
        }
      }
      return processed;
    }

    virtual void commit(bool flush) {
      _source->commit(flush);
    }

    virtual bool eof() const {
      return (queue_len() == 0) && _source->eof();
    }

    virtual size_t queue_len() const {
      return event_consumer<K, V>::queue_len();
    }

  private:
    std::shared_ptr<partition_source<K, V>> _source;
    predicate                               _predicate;
    //event_queue<kevent<K, V>>               _queue;
    metric_lag                              _lag;
    metric_counter                          _predicate_false;
  };
} // namespace