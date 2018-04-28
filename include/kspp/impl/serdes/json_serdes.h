#include <boost/uuid/uuid.hpp>
#include <ostream>
#include <istream>
#include <vector>
#include <typeinfo>
#pragma once

namespace kspp {
class json_serdes
{
  template<typename T> struct fake_dependency : public std::false_type {};

  public:
  json_serdes() {}

  static std::string name() { return "kspp::json"; }

  // does not use schema registry
  template<class T>
  int32_t register_schema(std::string name, const T& dummy){
    return 0;
  }

  template<class T>
  size_t encode(const T& src, std::ostream& dst) {
    static_assert(fake_dependency<T>::value, "you must use specialization to provide a encode for T");
  }

  template<class T>
  size_t decode(std::istream& src, T& dst) {
    static_assert(fake_dependency<T>::value, "you must use specialization to provide a decode for T");
  }
};
};

