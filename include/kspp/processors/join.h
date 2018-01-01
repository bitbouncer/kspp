#include <memory>
#include <deque>
#include <functional>
#include <kspp/kspp.h>

#pragma once

namespace kspp {
// alternative? replace with std::shared_ptr<const kevent<K, R>> left, std::shared_ptr<const kevent<K, R>> right, std::shared_ptr<kevent<K, R>> result;  

  template<class K, class leftV, class tableV>
  class left_join : public event_consumer<K, leftV>, public partition_source<K, std::pair<leftV, std::shared_ptr<tableV>>> {
  public:
    typedef std::pair<leftV, std::shared_ptr<tableV>> value_type;

    left_join(topology &t,
              std::shared_ptr<partition_source<K, leftV>> left,
    std::shared_ptr<materialized_source<K, tableV>> right)
    : event_consumer<K, leftV>()
    , partition_source<K, value_type>(left.get(), left->partition())
    , _left_stream (left)
    , _right_table(right) {
      _left_stream->add_sink([this](auto r) {
        this->_queue.push_back(r);
      });
      this->add_metric(&_lag);
    }

    ~left_join() {
      close();
    }

    std::string simple_name() const override {
      return "left_join";
    }

    void start(int64_t offset) override {
      // if we request begin - should we restart table here???
      //it seems that we should retain whatever's in the cache in as many cases as possible
      _right_table->start(kspp::OFFSET_STORED);
      _left_stream->start(offset);
    }

    void close() override {
      _right_table->close();
      _left_stream->close();
    }

    size_t queue_size() const override {
      return event_consumer<K, leftV>::queue_size();
    }

    size_t process(int64_t tick) override {
      if (_right_table->process(tick)>0)
        _right_table->commit(false);

      _left_stream->process(tick);

      size_t processed =0;
      // reuse event time & commit it from event stream
      while (this->_queue.next_event_time()<=tick) {
        auto left = this->_queue.pop_and_get();
        _lag.add_event_time(tick, left->event_time());
        ++processed;
        // null values from left should be ignored
        if (left->record() && left->record()->value()) {
          auto right_record = _right_table->get(left->record()->key());

          std::shared_ptr<tableV> right_val;
          if (right_record && right_record->value())
            right_val = std::make_shared<tableV>(*right_record->value());

          auto value = std::make_shared<value_type>(*left->record()->value(), right_val);
          auto record = std::make_shared<krecord<K, value_type>>(left->record()->key(), value, left->event_time());
          this->send_to_sinks(std::make_shared<kspp::kevent<K, value_type>>(record, left->id()));
        } else {
          // no output on left null
        }
      }
      return processed;
    }


    void commit(bool flush) override {
      _right_table->commit(flush);
      _left_stream->commit(flush);
    }

    bool eof() const override {
      return _right_table->eof() && _left_stream->eof();
    }

  private:
    std::shared_ptr<partition_source < K, leftV>>   _left_stream;
    std::shared_ptr<materialized_source < K, tableV>> _right_table;
    //value_joiner _value_joiner;
    metric_lag _lag;
  };


  template<class K, class leftV, class tableV>
  class inner_join : public event_consumer<K, leftV>, public partition_source<K, std::pair<leftV, tableV>> {
  public:
    typedef std::pair<leftV, tableV> value_type;

    inner_join(topology &t,
              std::shared_ptr<partition_source<K, leftV>> left,
    std::shared_ptr<materialized_source<K, tableV>> right)
    : event_consumer<K, leftV>()
    , partition_source<K, value_type>(left.get(), left->partition())
    , _left_stream (left)
    , _right_table(right) {
      _left_stream->add_sink([this](auto r) {
        this->_queue.push_back(r);
      });
      this->add_metric(&_lag);
    }

    ~inner_join() {
      close();
    }

    std::string simple_name() const override {
      return "inner_join";
    }

    void start(int64_t offset) override {
      // if we request begin - should we restart table here???
      //it seems that we should retain whatever's in the cache in as many cases as possible
      _right_table->start(kspp::OFFSET_STORED);
      _left_stream->start(offset);
    }

    void close() override {
      _right_table->close();
      _left_stream->close();
    }

    size_t queue_size() const override {
      return event_consumer<K, leftV>::queue_size();
    }

    size_t process(int64_t tick) override {
      if (_right_table->process(tick)>0)
        _right_table->commit(false);

      _left_stream->process(tick);

      size_t processed =0;
      // reuse event time & commit it from event stream
      while (this->_queue.next_event_time()<=tick) {
        auto left = this->_queue.pop_and_get();
        _lag.add_event_time(tick, left->event_time());
        ++processed;
        // null values from left should be ignored
        if (left->record() && left->record()->value()) {
          auto right_record = _right_table->get(left->record()->key());
          // null values from right should be ignored
          if (right_record && right_record->value()) {
            //auto right_val = std::make_shared<tableV>(*right_record->value());
            auto value = std::make_shared<value_type>(*left->record()->value(), *right_record->value());
            auto record = std::make_shared<krecord<K, value_type>>(left->record()->key(), value, left->event_time());
            this->send_to_sinks(std::make_shared<kspp::kevent<K, value_type>>(record, left->id()));
          }
        } else {
          // no output on left null
        }
      }
      return processed;
    }


    void commit(bool flush) override {
      _right_table->commit(flush);
      _left_stream->commit(flush);
    }

    bool eof() const override {
      return _right_table->eof() && _left_stream->eof();
    }

  private:
    std::shared_ptr<partition_source < K, leftV>>   _left_stream;
    std::shared_ptr<materialized_source < K, tableV>> _right_table;
    //value_joiner _value_joiner;
    metric_lag _lag;
  };

}