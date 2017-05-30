#include <assert.h>
#include <memory>
#include <functional>
#include <sstream>
#include <kspp/kspp.h>
#include <kspp/impl/serdes/text_serdes.h>
#pragma once

namespace kspp {

  template<class K, class V>
  void print(std::shared_ptr<kevent<K, V>> ev, std::ostream& _os, kspp::text_serdes* _codec) {
    _os << "ts: " << ev->event_time() << "  ";
    _codec->encode(ev->record()->key, _os);
    _os << ":";
    if (ev->record()->value)
      _codec->encode(*ev->record()->value, _os);
    else
      _os << "<nullptr>";
    _os << std::endl;
  }

  template<class V>
  void print(std::shared_ptr<kevent<void, V>> ev, std::ostream& _os, kspp::text_serdes* _codec) {
    _os << "ts: " << ev->event_time() << "  ";
    _codec->encode(*ev->record()->value, _os);
    _os << std::endl;
  }

  template<class K>
  void print(std::shared_ptr<kevent<K, void>> ev, std::ostream& _os, kspp::text_serdes* _codec) {
    _os << "ts: " << ev->event_time() << "  ";
    _codec->encode(ev->record()->key, _os);
    _os << std::endl;
  }

  template<class K, class V>
  class stream_sink : public partition_sink<K, V>
  {
  public:
    enum { MAX_KEY_SIZE = 1000 };

    stream_sink(topology_base& topology, std::shared_ptr<partition_source<K, V>> source, std::ostream* os)
      : partition_sink<K, V>(source->partition())
      , _os(*os)
      , _codec(std::make_shared<kspp::text_serdes>()) {
      source->add_sink(this);
    }

    virtual ~stream_sink() {
      this->flush();;
    }

    virtual std::string name() const {
      return "stream_sink";
    }

    virtual std::string processor_name() const {
      return "stream_sink";
    }

    virtual size_t queue_len() {
      return this->_queue.size();
    }

    virtual void commit(bool flush) {
      // noop
    }

    virtual bool process_one(int64_t tick) {
      size_t count = 0;
      while (this->_queue.size()) {
        auto ev = this->_queue.front();
        print(ev, _os, _codec.get());
        this->_queue.pop_front();
      }
      return (count > 0);
    }
 
    std::ostream&                      _os;
    std::shared_ptr<kspp::text_serdes> _codec;
  };



  //template<class K, class V>
  //class stream_sink : public partition_sink<K, V>
  //{
  //public:
  //  enum { MAX_KEY_SIZE = 1000 };

  //  stream_sink(topology_base& topology, std::shared_ptr<partition_source<K, V>> source, std::ostream* os)
  //    : partition_sink<K, V>(source->partition())
  //    , _os(*os)
  //    , _codec(std::make_shared<kspp::text_serdes>()) {
  //    source->add_sink(this);
  //  }

  //  virtual ~stream_sink() {
  //    this->flush();;
  //  }

  //  virtual std::string name() const {
  //    return "stream_sink";
  //  }

  //  virtual std::string processor_name() const {
  //    return "stream_sink";
  //  }

  //  virtual size_t queue_len() {
  //    return this->_queue.size();
  //  }

  //  virtual void commit(bool flush) {
  //    // noop
  //  }

  //  virtual bool process_one(int64_t tick) {
  //    size_t count = 0;
  //    while (this->_queue.size()) {
  //      auto ev = this->_queue.front();
  //      handle_event(ev);
  //      this->_queue.pop_front();
  //    }
  //    return (count > 0);
  //  }

  //private:
  //  void handle_event(std::shared_ptr<kevent<K, V>> ev) {
  //    _os << "ts: " << ev->event_time() << "  ";
  //    _codec->encode(ev->record()->key, _os);
  //    _os << ":";
  //    if (ev->record()->value)
  //      _codec->encode(*ev->record()->value, _os);
  //    else
  //      _os << "<nullptr>";
  //    _os << std::endl;
  //  }

  //  std::ostream&                      _os;
  //  std::shared_ptr<kspp::text_serdes> _codec;
  //};

  ////<null, VALUE>
  //template<class V>
  //class stream_sink<void, V> : public partition_sink<void, V>
  //{
  //public:
  //  stream_sink(topology_base& topology, std::shared_ptr<partition_source<void, V>> source, std::ostream* os)
  //    : partition_sink<void, V>(source.partition())
  //    , _os(*os)
  //    , _codec(std::make_shared<kspp::text_serdes>()) {
  //    source->add_sink(this);
  //  }

  //  virtual ~stream_sink() {
  //    this->flush();;
  //  }

  //  virtual std::string name() const {
  //    return "stream_sink";
  //  }

  //  virtual std::string processor_name() const {
  //    return "stream_sink";
  //  }

  //  virtual bool process_one(int64_t tick) {
  //    size_t count = 0;
  //    while (this->_queue.size()) {
  //      auto ev = this->_queue.front();
  //      handle_event(ev);
  //      this->_queue.pop_front();
  //    }
  //    return (count > 0);
  //  }
 
  //  virtual size_t queue_len() {
  //    return this->_queue.size();
  //  }

  //  virtual void commit(bool flush) {
  //    // noop
  //  }
  //   
  //private:
  //  void handle_event(std::shared_ptr<kevent<void, V>> ev) {
  //    _os << "ts: " << ev->event_time() << "  ";
  //    _codec->encode(*ev->record()->value, _os);
  //    _os << std::endl;
  //  }

  //  std::ostream&                      _os;
  //  std::shared_ptr<kspp::text_serdes> _codec;
  //};

  //// <key, nullptr>
  //template<class K>
  //class stream_sink<K, void> : public partition_sink<K, void>
  //{
  //public:
  //  stream_sink(topology_base& topology, std::shared_ptr<partition_source<K, void>> source, std::ostream* os)
  //    : partition_sink<K, void>(source->partition())
  //    , _os(*os)
  //    , _codec(std::make_shared<kspp::text_serdes>()) {
  //    source->add_sink(this);
  //  }

  //  virtual ~stream_sink() {
  //    this->flush();;
  //  }

  //  virtual std::string name() const {
  //    return "stream_sink";
  //  }

  //  virtual std::string processor_name() const {
  //    return "stream_sink";
  //  }

  //  virtual bool process_one(int64_t tick) {
  //    size_t count = 0;
  //    while (this->_queue.size()) {
  //      auto ev = this->_queue.front();
  //      handle_event(ev);
  //      this->_queue.pop_front();
  //    }
  //    return (count > 0);
  //  }

  //  virtual size_t queue_len() {
  //    return this->_queue.size();
  //  }

  //  virtual void commit(bool flush) {
  //    // noop
  //  }

  //private:
  //  void handle_event(std::shared_ptr<kevent<K, void>> ev) {
  //    _os << "ts: " << ev->event_time() << "  ";
  //    _codec->encode(ev->record()->key, _os);
  //    _os << std::endl;
  //  }

  //  std::ostream&                      _os;
  //  std::shared_ptr<kspp::text_serdes> _codec;
  //};

};