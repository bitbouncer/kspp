#pragma once
namespace kspp {
  template<class K, class V>
  class filter : public partition_source<K, V>
  {
  public:
    typedef std::function<bool(std::shared_ptr<krecord<K, V>> record)> predicate; // return true to keep

    filter(std::shared_ptr<partition_source<K, V>> source, predicate f)
      : partition_source<K, V>(source.get(), source->partition())
      , _source(source)
      , _predicate(f) {
      _source->add_sink([this](auto r) {
        _queue.push_back(r);
      });
      add_metrics(&_lag);
    }

    ~filter() {
      close();
    }

    virtual std::string processor_name() const { return "filter"; }

    static std::vector<std::shared_ptr<partition_source<K, V>>> create(std::vector<std::shared_ptr<partition_source<K, V>>>& streams, predicate f) {
      std::vector<std::shared_ptr<partition_source<K, V>>> res;
      for (auto i : streams)
        res.push_back(std::make_shared<filter<K, V>>(i, f));
      return res;
    }

    static std::shared_ptr<partition_source<K, V>> create(std::shared_ptr<partition_source<K, V>> source, predicate f) {
      return std::make_shared<filter<K, V>>(source, f);
    }

    std::string name() const {
      return _source->name() + "-filter";
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

    virtual bool process_one() {
      _source->process_one();
      bool processed = (_queue.size() > 0);
      while (_queue.size()) {
        auto r = _queue.front();
        _queue.pop_front();
        _lag.add_event_time(r->event_time);
        if (_predicate(r)) {
          this->send_to_sinks(r);
        }
      }
      return processed;
    }

    virtual void commit() {
      _source->commit();
    }

    virtual bool eof() const {
      return _source->eof() && (_queue.size() == 0);
    }

    virtual bool is_dirty() {
      return _source->is_dirty() || _queue.size()>0;
    }

    virtual size_t queue_len() {
      return _queue.size();
    }

  private:
    std::shared_ptr<partition_source<K, V>>    _source;
    predicate                                  _predicate;
    std::deque<std::shared_ptr<krecord<K, V>>> _queue;
    metrics_lag                                _lag;
  };
} // namespace