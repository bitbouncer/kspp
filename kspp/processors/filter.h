#pragma once
namespace csi {
  template<class K, class V>
  class filter : public partition_source<K, V>
  {
  public:
    typedef std::function<bool(std::shared_ptr<krecord<K, V>> record)> filter_fkn; // return true to keep

    filter(std::shared_ptr<partition_source<K, V>> source, filter_fkn f)
      : partition_source(source->partition())
      , _source(source)
      , _filter_fkn(f) {
      _source->add_sink([this](auto r) {
        _queue.push_back(r);
      });
    }

    ~filter() {
      close();
    }

    static std::vector<std::shared_ptr<partition_source<K, V>>> create(std::vector<std::shared_ptr<partition_source<K, V>>>& streams, filter_fkn f) {
      std::vector<std::shared_ptr<partition_source<K, V>>> res;
      for (auto i : streams)
        res.push_back(std::make_shared<filter<K, V>>(i, f));
      return res;
    }

    static std::shared_ptr<partition_source<K, V>> create(std::shared_ptr<partition_source<K, V>> source, filter_fkn f) {
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
        if (_filter_fkn(r))
          send_to_sinks(r);
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

    virtual std::string topic() const {
      return "internal-deque";
    }

  private:
    std::shared_ptr<partition_source<K, V>>    _source;
    filter_fkn                                 _filter_fkn;
    std::deque<std::shared_ptr<krecord<K, V>>> _queue;
  };
} // namespace