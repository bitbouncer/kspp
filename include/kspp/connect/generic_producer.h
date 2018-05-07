#include <kspp/kevent.h>

#pragma once

namespace kspp {
  template<class K, class V>
  class generic_producer {
  public:
    generic_producer() {
    }

    virtual ~generic_producer() {

    }

    virtual void close() =0;

    virtual bool eof() const =0;

    virtual void insert(std::shared_ptr<kspp::kevent<K, V>>)=0;

    virtual void poll()=0;

    virtual std::string topic() const =0;
  };
}
  
