#include <string>
#include <ostream>
#include <istream>
#include <boost/uuid/uuid.hpp>
#include <typeinfo>
#pragma once

namespace kspp {
class text_serdes
{
  template<typename T> struct fake_dependency : public std::false_type {};
  public:
  text_serdes() {}

  static std::string name() { return "kspp::text"; }

  template<class T>
  size_t encode(const T& src, std::ostream& dst) {
    static_assert(fake_dependency<T>::value, "you must use specialization to provide a encode for T");
  }

  template<class T>
  size_t decode(std::istream& src, T& dst) {
    static_assert(fake_dependency<T>::value, "you must use specialization to provide a decode for T");
  }
};

template<> inline size_t text_serdes::encode(const std::string& src, std::ostream& dst) {
  dst << src;
  return src.size();
}

template<> inline size_t text_serdes::decode(std::istream& src, std::string& dst) {
  dst.clear();
  std::getline(src, dst);
  return dst.size();
}

template<> inline size_t text_serdes::encode(const bool& src, std::ostream& dst) {
  dst << (src ? "true" : "false");
  return src ? 4 : 5;
}

template<> inline size_t text_serdes::decode(std::istream& src, bool& dst) {
  std::string s;
  std::getline(src, s);
  dst = (s == "true") ? true : false;
  return s.size();
}

template<> inline size_t text_serdes::encode(const int& src, std::ostream& dst) {
  auto s = std::to_string(src);
  dst << s;
  return s.size();
}

template<> inline size_t text_serdes::encode(const int64_t& src, std::ostream& dst) {
  auto s = std::to_string(src);
  dst << s;
  return s.size();
}

template<> inline size_t text_serdes::encode(const unsigned long& src, std::ostream& dst) {
  auto s = std::to_string(src);
  dst << s;
  return s.size();
}

template<> inline size_t text_serdes::encode(const uint64_t& src, std::ostream& dst) {
  auto s = std::to_string(src);
  dst << s;
  return s.size();
}

template<> inline size_t text_serdes::decode(std::istream& src, int& dst) {
  std::string s;
  std::getline(src, s);
  dst = std::atoi(s.c_str());
  return s.size();
}
};

