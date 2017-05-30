#include <memory>
#include <deque>
#include <functional>
#include <kspp/kspp.h>
#pragma once

namespace kspp {
// alternative? replace with std::shared_ptr<const kevent<K, R>> left, std::shared_ptr<const kevent<K, R>> right, std::shared_ptr<kevent<K, R>> result;  

template<class K, class streamV, class tableV, class R>
class left_join : public partition_source<K, R>
{
  public:
  typedef std::function<void(const K& key, const streamV& left, const tableV& right, R& result)> value_joiner;

  left_join(topology_base& topology, std::shared_ptr<partition_source<K, streamV>> stream, std::shared_ptr<materialized_source<K, tableV>> table, value_joiner f)
    : partition_source<K, R>(stream.get(), stream->partition())
    , _stream(stream)
    , _table(table)
    , _value_joiner(f) {
    _stream->add_sink([this](auto r) {
      _queue.push_back(r);
    });
    this->add_metric(&_lag);
  }

  ~left_join() {
    close();
  }

  virtual std::string processor_name() const { return "left_join"; }

  std::string name() const {
    return  _stream->name() + "-left_join(" + _table->name() + ")[" + type_name<K>::get() + ", " + type_name<R>::get() + "]";
  }

  virtual void start() {
    _table->start();
    _stream->start();
  }

  virtual void start(int64_t offset) {
    _table->start();
    _stream->start(offset);
  }

  virtual void close() {
    _table->close();
    _stream->close();
  }

  virtual size_t queue_len() {
    return _queue.size();
  }

  virtual bool process_one(int64_t tick) {
    if (!_table->eof()) {
      // just eat it... no join since we only joins with events????
      if (_table->process_one(tick))
        _table->commit(false);
      return true;
    }

    _stream->process_one(tick);

    if (!_queue.size())
      return false;

    while (_queue.size()) {
      auto t0 = _queue.front();
      _queue.pop_front();
      _lag.add_event_time(tick, t0->event_time());
      auto table_row = _table->get(t0->record()->key);
      if (table_row) {
        if (t0->record()->value) {
          auto record = std::make_shared<krecord<K, R>>(t0->record()->key, std::make_shared<R>(), t0->event_time());
          auto t1 = std::make_shared<kspp::kevent<K, R>>(record, t0->id());
          _value_joiner(t1->record()->key, *t0->record()->value, *table_row->value, *t1->record()->value);
          this->send_to_sinks(t1);
        } else {
          auto record = std::make_shared<krecord<K, R>>(t0->record()->key, nullptr, t0->event_time());
          auto t1 = std::make_shared<kspp::kevent<K, R>>(record, t0->id());
          this->send_to_sinks(t1);
        }
      } else {
        // join failed
      }
    }
    return true;
  }

  virtual void commit(bool flush) {
    _table->commit(flush);
    _stream->commit(flush);
  }

  virtual bool eof() const {
    return _table->eof() && _stream->eof();
  }

  private:
  std::shared_ptr<partition_source<K, streamV>>    _stream;
  std::shared_ptr<materialized_source<K, tableV>>  _table;
  std::deque<std::shared_ptr<kevent<K, streamV>>> _queue;
  value_joiner                                     _value_joiner;
  metric_lag                                       _lag;
};
}