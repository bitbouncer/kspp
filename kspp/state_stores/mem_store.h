#include "state_store.h"
#include <map> 

 namespace kspp {
 template<class K, class V, class CODEC=void>
  class mem_store 
    : public state_store<K, V> {
  public:
    class iterator_impl 
      : public kmaterialized_source_iterator_impl<K, V> {
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
        return (_it == _container.end()) ? nullptr : _it->second;
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

    mem_store(boost::filesystem::path storage_path){
    }

    virtual ~mem_store() {
    }

    static std::string type_name() {
      return "mem_store";
    }

    virtual void close() {
    }
    
    /**
    * Put a key-value pair if timestamp is greater or equal to existing record
    */
    virtual void insert(std::shared_ptr<krecord<K, V>> record) {
      _current_offset = std::max<int64_t>(_current_offset, record->offset);
      auto item = _store.find(record->key);

      // non existing - TBD should we keep a tombstone???
      if (item == _store.end()) {
        if (record->value)
          _store[record->key] = record;
        return;
      }

      // skip if we have a newer value
      if (item->second->event_time > record->event_time)
        return;

      if (record->value)
        item->second = record;
      else
        _store.erase(record->key);
    }
          
    /**
    * commits the offset
    */
    virtual void commit(bool flush) {
      // noop
    }
   
    /**
    * returns last offset
    */
    virtual int64_t offset() const {
      return _current_offset;
    }

    virtual void start(int64_t offset) {
      _current_offset = offset;
    }

    /**
    * Returns a key-value pair with the given key
    */
    virtual std::shared_ptr<krecord<K, V>> get(const K& key) {
      auto it = _store.find(key);
      return (it == _store.end()) ? nullptr : it->second;
    }

    virtual void clear() {
      _store.clear();
      _current_offset = -1;
    }

    virtual size_t size() const {
      return _store.size();
    }

    typename kspp::materialized_source<K, V>::iterator begin(void) {
      return typename kspp::materialized_source<K, V>::iterator(std::make_shared<iterator_impl>(_store, iterator_impl::BEGIN));
    }

    typename kspp::materialized_source<K, V>::iterator end() {
      return typename kspp::materialized_source<K, V>::iterator(std::make_shared<iterator_impl>(_store, iterator_impl::END));
    }

  private:
    std::map<K, std::shared_ptr<krecord<K, V>>> _store;
    int64_t                                     _current_offset;
  };
 }