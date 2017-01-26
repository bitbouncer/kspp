#include <assert.h>
#include <memory>
#include <functional>
#include <sstream>
#include <kspp/kspp.h>
#include <kspp/codecs/text_codec.h>
#pragma once

namespace kspp {

  template<class K, class V>
  class stream_sink : public partition_sink<K, V>
  {
  public:
    enum { MAX_KEY_SIZE = 1000 };

    stream_sink(topology_base& topology, std::shared_ptr<partition_source<K, V>> source, std::ostream* os)
      : partition_sink<K, V>(source->partition())
      , _os(*os)
      , _codec(std::make_shared<kspp::text_codec>()) {
      source->add_sink(this);
    }

    virtual ~stream_sink() {
      //_source->remove_sink()
    }
    
   virtual std::string name() const {
      return "stream_sink";
    }

    virtual int produce(std::shared_ptr<krecord<K, V>> r) {
      _os << "ts: " << r->event_time << "  ";
      _codec->encode(r->key, _os);
      _os << ":";
      if (r->value)
        _codec->encode(*r->value, _os);
      else
        _os << "<NULL>";
      _os << std::endl;
      return 0;
    }
    
    virtual size_t queue_len() { 
      return 0; 
    }

  private:
    std::ostream&                     _os;
    std::shared_ptr<kspp::text_codec> _codec;
  };

  //<null, VALUE>
  template<class V>
  class stream_sink<void, V> : public partition_sink<void, V>
  {
  public:
    stream_sink(topology_base& topology, std::shared_ptr<partition_source<void, V>> source, std::ostream* os)
      : partition_sink<void, V>(source.partition())
      , _os(*os)
      , _codec(std::make_shared<kspp::text_codec>()) {
      source->add_sink(this);
    }

    virtual ~stream_sink() {
      //_source->remove_sink()
    }
   
    virtual std::string name() const {
      return "stream_sink";
    }

    virtual int produce(std::shared_ptr<krecord<void, V>> r) {
      _os << "ts: " << r->event_time << "  ";
      _codec->encode(*r->value, _os);
      _os << std::endl;
      return 0;
    }

    virtual size_t queue_len() {
      return 0;
    }

  private:
    std::ostream&                     _os;
    std::shared_ptr<kspp::text_codec> _codec;
  };

  // <key, NULL>
  template<class K>
  class stream_sink<K, void> : public partition_sink<K, void>
  {
  public:
    stream_sink(topology_base& topology, std::shared_ptr<partition_source<K, void>> source, std::ostream* os)
      : partition_sink<K, void>(source->partition())
      , _os(*os)
      , _codec(std::make_shared<kspp::text_codec>()) {
      source->add_sink(this);
    }

    virtual ~stream_sink() {
      //_source->remove_sink()
    }

    virtual std::string name() const {
      return "stream_sink";
    }

    virtual int produce(std::shared_ptr<krecord<K, void>> r) {
      _os << "ts: " << r->event_time << "  ";
      _codec->encode(r->key, _os);
      _os << std::endl;
      return 0;
    }

    virtual size_t queue_len() {
      return 0;
    }

  private:
    std::ostream&                     _os;
    std::shared_ptr<kspp::text_codec> _codec;
  };

};