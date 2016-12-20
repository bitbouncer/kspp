#include <string>
#include <ostream>
#include <istream>
#include <boost/uuid/uuid.hpp>
#pragma once

namespace csi {
 /* inline size_t binary_encode(const int64_t& a, std::ostream& dst) {
  dst.write((const char*)&a, sizeof(int64_t));
  return sizeof(int64_t);
  }

  inline size_t binary_decode(std::istream& src, int64_t& dst) {
  src.read((char*)&dst, sizeof(int64_t));
  return src.good() ? sizeof(int64_t) : 0;
  }

  inline size_t binary_encode(const int32_t& a, std::ostream& dst) {
    dst.write((const char*) &a, sizeof(int32_t));
    return sizeof(int32_t);
  }

  inline size_t binary_decode(std::istream& src, int32_t& dst) {
    src.read((char*) &dst, sizeof(int32_t));
    return src.good() ? sizeof(int32_t) : 0;
  }


  inline size_t binary_encode(const boost::uuids::uuid& a, std::ostream& dst) {
    dst.write((const char*)a.data, 16);
    return 16;
  }

  inline size_t binary_decode(std::istream& src, boost::uuids::uuid& dst) {
    src.read((char*)dst.data, 16);
    return src.good() ? 16 : 0;
  }

  inline size_t binary_encode(text_codec* unused, const std::string& a, std::ostream& dst) {
    uint32_t sz = (uint32_t) a.size();
    dst.write((const char*) &sz, sizeof(uint32_t));
    dst.write((const char*) a.data(), sz);
    return sz + sizeof(uint32_t);
  }*/

class text_codec
{
  public:
  text_codec() {}

  static std::string name() { return "text"; }

  template<class T>
  size_t encode(const T& src, std::ostream& dst) {
    return encode(this, src, dst);
  }

  template<class T>
  size_t decode(std::istream& src, T& dst) {
    return decode(this, src, dst);
  }

  
 /* template<>
  static size_t encode(const int64_t& src, std::ostream& dst) {
    dst << src ? "true" : "false";
    return src ? 4 : 5;
  }

  template<>
  static size_t decode(std::istream& src, int64_t& dst) {
    std::string s;
    std::getline(src, s);
    dst = atoi()
    return s.size();
  }*/

  /* inline size_t binary_encode(const int64_t& a, std::ostream& dst) {
  dst.write((const char*)&a, sizeof(int64_t));
  std::to_string
  return sizeof(int64_t);
  }

  inline size_t binary_decode(std::istream& src, int64_t& dst) {
  src.read((char*)&dst, sizeof(int64_t));
  return src.good() ? sizeof(int64_t) : 0;
  }
  */

};

template<> size_t text_codec::encode(const std::string& src, std::ostream& dst) {
  dst << src;
  return src.size();
}

template<> size_t text_codec::decode(std::istream& src, std::string& dst) {
  dst.clear();
  std::getline(src, dst);
  return dst.size();
}

template<> size_t text_codec::encode(const bool& src, std::ostream& dst) {
  dst << src ? "true" : "false";
  return src ? 4 : 5;
}

template<> size_t text_codec::decode(std::istream& src, bool& dst) {
  std::string s;
  std::getline(src, s);
  dst = (s == "true") ? true : false;
  return s.size();
}

template<> size_t text_codec::encode(const int& src, std::ostream& dst) {
  auto s = std::to_string(src);
  dst << s;
  return s.size();
}

template<> size_t text_codec::decode(std::istream& src, int& dst) {
  std::string s;
  std::getline(src, s);
  dst = std::atoi(s.c_str());
  return s.size();
}
};

