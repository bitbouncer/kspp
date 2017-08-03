#include <functional>
#pragma once

namespace csi {
namespace async {
template <typename Value>
class destructor_callback
{
  public:
  destructor_callback(std::function <void(Value& val)>  callback) : 
    _cb(callback) 
  {}

  destructor_callback(const Value& initial_value, std::function <void(Value& val)>  callback) : 
    _val(initial_value), 
    _cb(callback) 
  {}

  ~destructor_callback() {
    if (_cb) {
      try {
        _cb(_val);
      }
      catch (...) {
        // ignore this
        assert(false);
      }
    }
  }
  
  inline Value& value() { 
    return _val; 
  }

  inline const Value& value() const { 
    return _val; 
  }
  
  private:
  std::function <void(Value& ec)> _cb;
  Value _val;
};
} // namespace
}// namespace