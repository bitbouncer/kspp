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

    partition_stream_sink(std::shared_ptr<partition_source<K, V>> source, size_t partition, std::ostream& os)
      : partition_sink<K, V>(partition)
      , _source(source)
      , _os(os)
      , _codec(std::make_shared<kspp::text_codec>()) {
      _source->add_sink([this](auto r) {
        _os << "ts: " << r->event_time << "  ";
        _codec->encode(r->key, _os);
        _os << ":";
        if (r->value)
          _codec->encode(*r->value, _os);
        else
          _os << "<NULL>";
        _os << std::endl;
      });
    }

    virtual ~partition_stream_sink() {
      //_source->remove_sink()
    }

    static std::shared_ptr<kspp::partition_stream_sink<K, V>> create(std::shared_ptr<partition_source<K, V>> source, size_t partition, std::ostream& os) {
      return std::make_shared<kspp::partition_stream_sink<K, V>>(source, partition, os);
    }

    virtual std::string kspp::partition_processor::name() const {
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
    std::shared_ptr<partition_source<K, V>> _source;
    std::ostream&                           _os;
    std::shared_ptr<kspp::text_codec>       _codec;
  };

  //<null, VALUE>
  template<class V>
  class partition_stream_sink<void, V> : public partition_sink<void, V>
  {
  public:
    partition_stream_sink(std::shared_ptr<partition_source<void, V>> source, size_t partition, std::ostream& os)
      : partition_sink<void, V>(partition)
      , _source(source)
      , _os(os)
      , _codec(std::make_shared<kspp::text_codec>()) {
      _source->add_sink([this](auto r) {
        _os << "ts: " << r->event_time << "  ";
        _codec->encode(*r->value, _os);
        _os << std::endl;
      });
    }

    virtual ~partition_stream_sink() {
      //_source->remove_sink()
    }

    static std::shared_ptr<kspp::partition_stream_sink<void, V>> create(std::shared_ptr<partition_source<void, V>> source, size_t partition, std::ostream& os) {
      return std::make_shared<kspp::partition_stream_sink<void, V>>(source, partition, os);
    }
    
    virtual std::string kspp::partition_processor::name() const {
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
    std::shared_ptr<partition_source<void, V>> _source;
    std::ostream&                              _os;
    std::shared_ptr<kspp::text_codec>          _codec;
  };

  // <key, NULL>
  template<class K>
  class partition_stream_sink<K, void> : public partition_sink<K, void>
  {
  public:
    partition_stream_sink(std::shared_ptr<partition_source<K, void>> source, size_t partition, std::ostream& os)
      : partition_sink<K, void>(partition)
      , _source(source)
      , _os(os)
      , _codec(std::make_shared<kspp::text_codec>()) {
      _source->add_sink([this](auto r) {
        _os << "ts: " << r->event_time << "  ";
        _codec->encode(r->key, _os);
        _os << std::endl;
      });
    }

    virtual ~partition_stream_sink() {
      //_source->remove_sink()
    }

    static std::shared_ptr<kspp::partition_stream_sink<K, void>> create(std::shared_ptr<partition_source<K, void>> source, size_t partition, std::ostream& os) {
      return std::make_shared<kspp::partition_stream_sink<K, void>>(source, partition, os);
    }

    virtual std::string kspp::partition_processor::name() const {
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
    std::shared_ptr<partition_source<K, void>> _source;
    std::ostream&                              _os;
    std::shared_ptr<kspp::text_codec>          _codec;
  };

};