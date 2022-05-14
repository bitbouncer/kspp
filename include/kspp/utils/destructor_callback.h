#include <functional>
#include <cassert>

#pragma once

namespace kspp {
  namespace async {
    template<typename Value>
    class destructor_callback {
    public:
      destructor_callback(std::function<void(Value &val)> callback) :
          cb_(callback) {}

      destructor_callback(const Value &initial_value, std::function<void(Value &val)> callback) :
          val_(initial_value),
          cb_(callback) {}

      ~destructor_callback() {
        if (cb_) {
          try {
            _cb(val_);
          }
          catch (...) {
            // ignore this
            assert(false);
          }
        }
      }

      inline Value &value() {
        return val_;
      }

      inline const Value &value() const {
        return val_;
      }

    private:
      std::function<void(Value &ec)> cb_;
      Value val_;
    };
  } // namespace
}// namespace