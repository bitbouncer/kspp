#include <memory>
#include <deque>
#include <functional>
#include <kspp/kspp.h>
#pragma once

namespace kspp {
  //maybe we should materilize the join so we can do get...  
  //then we can change type to kstream
  // maybee we should remove partition_sink and just add a callback... like in transform TBD
  // why do we need materialized event stream??? is it not enought witha partition_stream???
  template<class K, class streamV, class tableV, class R>
  //class left_join : public partition_source<K, R>, partition_sink<K, streamV>
  class left_join : public partition_source<K, R>
  {
  public:
    typedef std::function<void(const K& key, const streamV& left, const tableV& right, R& result)> value_joiner; // TBD replace with std::shared_ptr<const krecord<K, R>> left, std::shared_ptr<const krecord<K, R>> right, std::shared_ptr<krecord<K, R>> result;

    left_join(topology_base& topology, std::shared_ptr<partition_source<K, streamV>> stream, std::shared_ptr<ktable_partition<K, tableV>> table, value_joiner f)
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

    virtual bool process_one() {
      if (!_table->eof()) {
        // just eat it... no join since we only joins with events????
        if (_table->process_one())
          _table->commit();
        return true;
      }

      _stream->process_one();

      if (!_queue.size())
        return false;

      while (_queue.size()) {
        auto e = _queue.front();
        _queue.pop_front();
        _lag.add_event_time(e->event_time);
        auto table_row = _table->get(e->key);
        if (table_row) {
          if (e->value) {
            auto p = std::make_shared<kspp::krecord<K, R>>(e->key, std::make_shared<R>(), e->event_time);
            _value_joiner(e->key, *e->value, *table_row->value, *p->value);
            this->send_to_sinks(p);
          } else {
            auto p = std::make_shared<kspp::krecord<K, R>>(e->key);
            p->event_time = e->event_time;
            p->offset = e->offset;
            this->send_to_sinks(p);
          }
        } else {
          // join failed
        }
      }
      return true;
    }

    virtual void commit() {
      _table->commit();
      _stream->commit();
    }

    virtual bool eof() const {
      return _table->eof() && _stream->eof();
    }

  private:
    std::shared_ptr<partition_source<K, streamV>>    _stream;
    std::shared_ptr<ktable_partition<K, tableV>>     _table; // ska denna vara här överhuvudtaget - räcker det inte med att addera den som sink?
    std::deque<std::shared_ptr<krecord<K, streamV>>> _queue;
    value_joiner                                     _value_joiner;
    metric_lag                                       _lag;
  };
}