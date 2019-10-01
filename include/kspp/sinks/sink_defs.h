#include <functional>

#pragma once

namespace kspp {
  template<class K>
  class kafka_partitioner_base {
  public:
    using partitioner = typename std::function<uint32_t(const K &key)>;
  };

  template<>
  class kafka_partitioner_base<void> {
  public:
    using partitioner = typename std::function<uint32_t(void)>;
  };
}



