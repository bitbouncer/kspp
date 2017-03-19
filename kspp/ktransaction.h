#include "krecord.h"
#include <cstdint>
#include <memory>
#include "impl/commit_chain.h"
#pragma once

namespace kspp {
template<class K, class V>
class ktransaction
{
  public:
  ktransaction(std::shared_ptr<krecord<K, V>> r, std::shared_ptr<commit_chain::autocommit_marker> autocommit_marker =nullptr)
    : _record(r)
    , _autocommit_marker(autocommit_marker) {
  }

  inline int64_t event_time() const {
    return _record ? _record->event_time : -1;
  }

  inline int64_t offset() const {
    return _autocommit_marker ? _autocommit_marker->offset() : -1;
  }

  inline std::shared_ptr<krecord<K, V>> record() { 
    return _record; 
  }

  inline std::shared_ptr<commit_chain::autocommit_marker> id() {
    return _autocommit_marker;
  }

  std::shared_ptr<krecord<K, V>>                   _record;
  std::shared_ptr<commit_chain::autocommit_marker> _autocommit_marker;
};
}