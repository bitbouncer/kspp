#include <assert.h>
#include <memory>
#include <functional>
#include <sstream>
#include "kspp_defs.h"
#include <kspp/codecs/text_codec.h>
#pragma once

namespace csi {

template<class K, class V>
class stream_sink : public ksink<K, V>
{
  public:
  enum { MAX_KEY_SIZE = 1000 };

  stream_sink(std::ostream& os) 
  : _os(os) 
  , _codec(std::make_shared<csi::text_codec>()){
  }
  
  virtual ~stream_sink() { 
    close(); 
  }

  virtual std::string name() const {
    return "stream_sink";
  }

  virtual void close() {
  }

  virtual size_t queue_len() {
    return 0;
  }

  virtual void poll(int timeout) {
    return;
  }

  virtual int produce(std::shared_ptr<krecord<K, V>> r) {
    _codec->encode(r->key, _os);
    _os << ":";
    if (r->value)
      _codec->encode(*r->value, _os);
    else
      _os << "<NULL>";
    _os << std::endl;
    return 1;
  }

  private:
  std::ostream&                     _os;
  std::shared_ptr<csi::text_codec>  _codec;
};

//<null, VALUE>
template<class V>
class stream_sink<void, V> : public ksink<void, V>
{
  public:
  stream_sink(std::ostream& os)
    : _os(os)
    , _codec(std::make_shared<csi::text_codec>()) {}

  virtual ~stream_sink() {
    close();
  }

  virtual std::string name() const {
    return "stream_sink";
  }

  virtual void close() {}

  virtual size_t queue_len() {
    return 0;
  }

  virtual void poll(int timeout) {
    return;
  }

  virtual int produce(std::shared_ptr<krecord<void, V>> r) {
    if (r->value) {
      _codec->encode(*r->value, _os);
    } else {
      _os << "<NULL>";
    }
    _os << std::endl;
    return 1;
  }

  private:
  std::ostream&                     _os;
  std::shared_ptr<csi::text_codec>  _codec;
};

// <key, NULL>
template<class K>
class stream_sink<K, void> : public ksink<K, void>
{
  public:
  stream_sink(std::ostream& os)
    : _os(os)
    , _codec(std::make_shared<csi::text_codec>()) {}

  virtual ~stream_sink() {
    close();
  }

  virtual std::string name() const {
    return "stream_sink";
  }

  virtual void close() {}

  virtual size_t queue_len() {
    return 0;
  }

  virtual void poll(int timeout) {
    return;
  }

  virtual int produce(std::shared_ptr<krecord<K, void>> r) {
    ksize = _codec->encode(r->key, _os);
    _os << std::endl;
    return 1;
  }

  private:
  std::ostream&                     _os;
  std::shared_ptr<csi::text_codec>  _codec;
};

};