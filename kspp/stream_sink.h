#include <assert.h>
#include <memory>
#include <functional>
#include <sstream>
#include "kspp_defs.h"
#include <kspp/codecs/text_codec.h>
#pragma once

namespace csi {

template<class K, class V>
class stream_sink
{
  public:
  enum { MAX_KEY_SIZE = 1000 };

  stream_sink(std::shared_ptr<partition_source<K, V>> source, std::ostream& os)
  : _source(source)
  , _os(os) 
  , _codec(std::make_shared<csi::text_codec>()){
    _source->add_sink([this](auto r) {
      _codec->encode(r->key, _os);
      _os << ":";
      if (r->value)
        _codec->encode(*r->value, _os);
      else
        _os << "<NULL>";
      _os << std::endl;
    });
  }
  
  virtual ~stream_sink() { 
    //_source->remove_sink()
  }

  private:
  std::shared_ptr<partition_source<K, V>> _source;
  std::ostream&                             _os;
  std::shared_ptr<csi::text_codec>          _codec;
};

//<null, VALUE>
template<class V>
class stream_sink<void, V>
{
  public:
    stream_sink(std::shared_ptr<partition_source<void, V>> source, std::ostream& os)
    : _source(source)
      , _os(os)
    , _codec(std::make_shared<csi::text_codec>()) {
    _source->add_sink([this](auto r) {
      _codec->encode(*r->value, _os);
      _os << std::endl;
    });
  }

  virtual ~stream_sink() {
    //_source->remove_sink()
  }


  private:
  std::shared_ptr<partition_source<void, V>> _source;
  std::ostream&                              _os;
  std::shared_ptr<csi::text_codec>           _codec;
};

// <key, NULL>
template<class K>
class stream_sink<K, void>
{
  public:
    stream_sink(std::shared_ptr<partition_source<K, void>> source, std::ostream& os)
    : _source(source)
    , _os(os)
    , _codec(std::make_shared<csi::text_codec>()) {
      _source->add_sink([this](auto r) {
        _codec->encode(r->key, _os);
        _os << std::endl;
      });
    }

  virtual ~stream_sink() {
    //_source->remove_sink()
  }

  private:
  std::shared_ptr<partition_source<K, void>> _source;
  std::ostream&                              _os;
  std::shared_ptr<csi::text_codec>           _codec;
};

};