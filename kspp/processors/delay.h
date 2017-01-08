#pragma once
namespace csi {
  template<class K, class V>
  class delay : public partition_source<K, V>
  {
  public:
    typedef std::function<bool(std::shared_ptr<krecord<K, V>> record)> predicate; // return true to keep

    delay(std::shared_ptr<partition_source<K, V>> source, int ms)
      : partition_source(source->partition())
      , _source(source)
      , _delay(ms) {
      _source->add_sink([this](auto r) {
        _queue.push_back(r);
      });
    }

    ~delay() {
      close();
    }

    static std::vector<std::shared_ptr<partition_source<K, V>>> create(std::vector<std::shared_ptr<partition_source<K, V>>>& streams, int ms) {
      std::vector<std::shared_ptr<partition_source<K, V>>> res;
      for (auto i : streams)
        res.push_back(std::make_shared<delay<K, V>>(i, ms));
      return res;
    }

    static std::shared_ptr<partition_source<K, V>> create(std::shared_ptr<partition_source<K, V>> source, int ms) {
      return std::make_shared<delay<K, V>>(source, f);
    }

    std::string name() const {
      return _source->name() + "-delay";
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
      if (_queue.size()==0)
        _source->process_one();

      if (_queue.size() == 0)
        return false;

      auto r = _queue.front();
      if (r->event_time + _delay > milliseconds_since_epoch()) {
        _queue.pop_front();
        send_to_sinks(r);
        return true;
      }

      return false;
    }

    virtual void commit() {
      _source->commit();
    }

    virtual bool eof() const {
      return (_queue.size()==0) && _source->eof());
    }

    virtual size_t queue_len() {
      return _queue.size();
    }

    virtual std::string topic() const {
      return "internal-deque";
    }

  private:
    std::shared_ptr<partition_source<K, V>>    _source;
    int                                        _delay;
    std::deque<std::shared_ptr<krecord<K, V>>> _queue;
  };
} // namespace