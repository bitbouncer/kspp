#include <map>
#include <kspp/kspp.h>
#pragma once

namespace kspp {
  template<class K, class V>
  class ktable_mem : public ktable<K, V>
  {
  public:
    class iterator_impl : public kmaterialized_source_iterator_impl<K, V>
    {
    public:
      enum seek_pos_e { BEGIN, END };

      iterator_impl(std::map<K, std::shared_ptr<krecord<K, V>>>& container, seek_pos_e pos)
        : _container(container)
        , _it(pos == BEGIN ? _container.begin() : _container.end()) {
      }

      virtual bool valid() const {
        return  _it != _container.end();
      }

      virtual void next() {
        if (_it == _container.end())
          return;
        ++_it;
      }

      virtual std::shared_ptr<krecord<K, V>> item() const {
        return (_it == _container.end()) ? nullptr :  _it->second;
      }

      virtual bool operator==(const kmaterialized_source_iterator_impl<K, V>& other) const {
        if (valid() && !other.valid())
          return false;
        if (!valid() && !other.valid())
          return true;
        if (valid() && other.valid())
          return _it->first == ((const iterator_impl&)other)._it->first;
        return false;
      }

    private:
      std::map<K, std::shared_ptr<krecord<K, V>>>& _container;
      typename std::map<K, std::shared_ptr<krecord<K, V>>>::iterator _it;
    };
    
    ktable_mem(topology_base& topology, std::shared_ptr<kspp::partition_source<K, V>> source)
      : ktable<K, V>(source.get())
      , _source(source)
      , _current_offset(RdKafka::Topic::OFFSET_BEGINNING)
      , _count("count") {
      _source->add_sink([this](auto record) {
        _current_offset = record->offset;
        _lag.add_event_time(record->event_time);
        ++_count;
        if (record->value)
          _state_store[record->key] = record;
        else
          _state_store.erase(record->key);
        this->send_to_sinks(record);
      });
      this->add_metric(&_lag);
      this->add_metric(&_count);
    }

    virtual ~ktable_mem() {
      close();
    }

    virtual std::string name() const {
      return   _source->name() + "-ktable_mem";
    }

    virtual std::string processor_name() const { return "ktable_mem"; }

    virtual void start() {
      _source->start(_current_offset);
    }

    virtual void start(int64_t offset) {
      _current_offset = offset;
      _source->start(_current_offset);
    }

    virtual void commit() {
      // noop???
    }

    virtual void close() {
      _source->close();
      _state_store.clear();
    }

    virtual bool eof() const {
      return _source->eof();
    }

    virtual bool process_one() {
      return _source->process_one();
    }

    virtual void flush_offset() {
      //noop
    }

    inline int64_t offset() const {
      return _current_offset;
    }

    // inherited from kmaterialized_source
    virtual std::shared_ptr<krecord<K, V>> get(const K& key) {
      auto it = _state_store.find(key);
      return (it == _state_store.end()) ? nullptr : it->second;
    }

    typename kspp::materialized_partition_source<K, V>::iterator begin(void) {
      return typename kspp::materialized_partition_source<K, V>::iterator(std::make_shared<iterator_impl>(_state_store, iterator_impl::BEGIN));
    }

    typename kspp::materialized_partition_source<K, V>::iterator end() {
      return typename kspp::materialized_partition_source<K, V>::iterator(std::make_shared<iterator_impl>(_state_store, iterator_impl::END));
    }

  private:
    std::shared_ptr<kspp::partition_source<K, V>> _source;
    std::map<K, std::shared_ptr<krecord<K, V>>>   _state_store;
    int64_t                                       _current_offset;
    metric_lag                                    _lag;
    metric_counter                                _count;
  };
};

