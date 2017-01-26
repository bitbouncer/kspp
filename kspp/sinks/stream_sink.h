#include <assert.h>
#include <memory>
#include <functional>
#include <sstream>
#include <kspp/kspp.h>
#include <kspp/codecs/text_codec.h>
#pragma once

namespace kspp {

  template<class K, class V>
  class partition_stream_sink : public partition_sink<K, V>
  {
  public:
    enum { MAX_KEY_SIZE = 1000 };

    partition_stream_sink(partition_topology_base& topology, std::ostream& os)
      : partition_sink<K, V>(topology.partition())
      , _os(os)
      , _codec(std::make_shared<kspp::text_codec>()) {
    }

    virtual ~partition_stream_sink() {
      //_source->remove_sink()
    }
    
    // from kspp::partition_processor::name()
    virtual std::string name() const {
      return "partition_stream_sink";
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
  class partition_stream_sink<void, V> : public partition_sink<void, V>
  {
  public:
    partition_stream_sink(partition_topology_base& topology, std::ostream& os)
      : partition_sink<void, V>(topology.partition())
      , _os(os)
      , _codec(std::make_shared<kspp::text_codec>()) {
    }

    virtual ~partition_stream_sink() {
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
  class partition_stream_sink<K, void> : public partition_sink<K, void>
  {
  public:
    partition_stream_sink(partition_topology_base& topology, std::ostream& os)
      : partition_sink<K, void>(topology.partition())
      , _os(os)
      , _codec(std::make_shared<kspp::text_codec>()) {
    }

    virtual ~partition_stream_sink() {
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