#pragma once
namespace kspp {
  template<class K, class V>
  class delay : public partition_source<K, V>
  {
  public:
    typedef std::function<bool(std::shared_ptr<ktransaction<K, V>> record)> predicate; // return true to keep

    delay(topology_base& topology, std::shared_ptr<partition_source<K, V>> source, int ms)
      : partition_source<K, V>(source->partition())
      , _source(source)
      , _delay(ms) {
      _source->add_sink([this](auto r) {
        _queue.push_back(r);
      });
      this->add_metric(&_lag);
    }

    ~delay() {
      close();
    }

    virtual std::string processor_name() const { return "delay"; }

    std::string name() const {
      return _source->name() + "-delay(" + std::to_string(_delay) + ")";
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
        _source->process_one();

      if (_queue.size() == 0)
        return false;

      auto r = _queue.front();
      _lag.add_event_time(tick, r->event_time);
      if (r->event_time + _delay > tick) {
        _queue.pop_front();
        this->send_to_sinks(r);
        return true;
      }

      return false;
    }

    virtual void commit(flush) {
      _source->commit(flush);
    }

    virtual bool eof() const {
      return (_queue.size()==0) && _source->eof());
    }

    virtual size_t queue_len() {
      return _queue.size();
    }

  private:
    std::shared_ptr<partition_source<K, V>>    _source;
    int                                        _delay;
    std::deque<std::shared_ptr<ktransaction<K, V>>> _queue;
    metric_lag                                 _lag;
  };
} // namespace