#include <boost/filesystem.hpp>
#include "kstream.h"
#pragma once

namespace csi {
  template<class K, class V, class codec>
  class ktable : public kstream<K, V, codec>
  {
  public:
    ktable(std::string nodeid, std::string brokers, std::string topic, int32_t partition, std::string storage_path, std::shared_ptr<codec> codec) :
      kstream(nodeid, brokers, topic, partition, storage_path, codec) {}
  };
};


