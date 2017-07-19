#include <cstdint>
#include <string>
#include <boost/uuid/uuid.hpp>

#pragma once

namespace kspp {
// default implementation
  template<typename T>
  struct type_name {
    static std::string get() {
      return typeid(T).name();
    }
  };

  template<>
  struct type_name<bool> { static inline const std::string get() { return "bool"; }};

  template<>
  struct type_name<int32_t> { static inline const std::string get() { return "int32_t"; }};

  template<>
  struct type_name<int64_t> { static inline const std::string get() { return "int64_t"; }};

  template<>
  struct type_name<size_t> { static inline const std::string get() { return "size_t"; }};

  template<>
  struct type_name<std::string> { static inline const std::string get() { return "string"; }};

  template<>
  struct type_name<boost::uuids::uuid> { static inline const std::string get() { return "uuid"; }};
}; // namespace
