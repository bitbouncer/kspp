#include <assert.h>
#include <memory>
#include <functional>
#include <sstream>
#include <kspp/kspp.h>
#include <kspp/impl/serdes/text_serdes.h>
#pragma once

namespace kspp {
  template<class K, class V>
  void stream_sink_print(std::shared_ptr<kevent<K, V>> ev, std::ostream& _os, kspp::text_serdes* _codec) {
    _os << "ts: " << ev->event_time() << "  ";
    _codec->encode(ev->record()->key(), _os);
    _os << ":";
    if (ev->record()->value())
      _codec->encode(*ev->record()->value(), _os);
    else
      _os << "<nullptr>";
    _os << std::endl;
  }

  template<class V>
  void stream_sink_print(std::shared_ptr<kevent<void, V>> ev, std::ostream& _os, kspp::text_serdes* _codec) {
    _os << "ts: " << ev->event_time() << "  ";
    _codec->encode(*ev->record()->value(), _os);
    _os << std::endl;
  }

  template<class K>
  void stream_sink_print(std::shared_ptr<kevent<K, void>> ev, std::ostream& _os, kspp::text_serdes* _codec) {
    _os << "ts: " << ev->event_time() << "  ";
    _codec->encode(ev->record()->key(), _os);
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

    virtual std::string simple_name() const {
      return "stream_sink";
    }

    virtual size_t queue_len() const {
      return event_consumer<K, V>::queue_len();
    }

    virtual void commit(bool flush) {
      // noop
    }

    virtual bool process_one(int64_t tick) {
      size_t count = 0;
      while (this->_queue.size()) {
        auto ev = this->_queue.front();
        stream_sink_print(ev, _os, _codec.get());
        this->_queue.pop_front();
      }
      return (count > 0);
    }
 
    std::ostream&                      _os;
    std::shared_ptr<kspp::text_serdes> _codec;
  };
};