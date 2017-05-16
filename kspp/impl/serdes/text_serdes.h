#include <string>
#include <ostream>
#include <istream>
#include <strstream>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <boost/uuid/string_generator.hpp>
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
  inline size_t decode(const char* payload, size_t size, T& dst) {
    std::istrstream src(payload, size);
    return decode(src, dst);
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

template<> inline size_t text_serdes::decode(std::istream& src, int& dst) {
  std::string s;
  std::getline(src, s);
  dst = std::atoi(s.c_str());
  return s.size();
}

template<> inline size_t text_serdes::encode(const long& src, std::ostream& dst) {
  auto s = std::to_string(src);
  dst << s;
  return s.size();
}

template<> inline size_t text_serdes::decode(std::istream& src, long& dst) {
  std::string s;
  std::getline(src, s);
  dst = std::atol(s.c_str());
  return s.size();
}

template<> inline size_t text_serdes::encode(const long long& src, std::ostream& dst) {
  auto s = std::to_string(src);
  dst << s;
  return s.size();
}

template<> inline size_t text_serdes::decode(std::istream& src, long long& dst) {
  std::string s;
  std::getline(src, s);
  dst = std::atoll(s.c_str());
  return s.size();
}

template<> inline size_t text_serdes::encode(const unsigned int& src, std::ostream& dst) {
  auto s = std::to_string(src);
  dst << s;
  return s.size();
}

template<> inline size_t text_serdes::decode(std::istream& src, unsigned int& dst) {
  std::string s;
  std::getline(src, s);
  dst = (unsigned int) strtoul(s.c_str(), 0, 10);
  return s.size();
}

template<> inline size_t text_serdes::encode(const unsigned long& src, std::ostream& dst) {
  auto s = std::to_string(src);
  dst << s;
  return s.size();
}

template<> inline size_t text_serdes::decode(std::istream& src, unsigned long& dst) {
  std::string s;
  std::getline(src, s);
  dst = strtoul(s.c_str(), 0, 10);
  return s.size();
}

template<> inline size_t text_serdes::encode(const unsigned long long& src, std::ostream& dst) {
  auto s = std::to_string(src);
  dst << s;
  return s.size();
}

template<> inline size_t text_serdes::decode(std::istream& src, unsigned long long& dst) {
  std::string s;
  std::getline(src, s);
  dst = strtoull(s.c_str(), 0, 10);
  return s.size();
}

template<> inline size_t text_serdes::encode(const boost::uuids::uuid& src, std::ostream& dst) {
  std::string s = boost::uuids::to_string(src);
  dst << s;
  return s.size();
}

template<> inline size_t text_serdes::decode(std::istream& src, boost::uuids::uuid& dst) {
  static boost::uuids::string_generator gen;
  std::string s;
  std::getline(src, s);
  dst = gen(s);
  return s.size();
}

};

