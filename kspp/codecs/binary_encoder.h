#pragma once
#include <boost/uuid/uuid.hpp>
#include <ostream>
#include <istream>

namespace csi {
  inline size_t binary_encode(const int64_t& a, std::ostream& dst) {
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

  inline size_t binary_encode(bool a, std::ostream& dst) {
    if (a) {
      char one = 0x01;
      dst.write((const char*) &one, 1);
    } else {
      char zero = 0x00;
      dst.write((const char*) &zero, 1);
    }
    return sizeof(1);
  }

  inline size_t binary_decode(std::istream& src, bool& dst) {
    char ch;
    src.read((char*) &ch, 1);
    dst = (ch == 0x00) ? false : true;
    return src.good() ? 1 : 0;
  }

  inline size_t binary_encode(const boost::uuids::uuid& a, std::ostream& dst) {
    dst.write((const char*)a.data, 16);
    return 16;
  }

  inline size_t binary_decode(std::istream& src, boost::uuids::uuid& dst) {
    src.read((char*)dst.data, 16);
    return src.good() ? 16 : 0;
  }

  inline size_t binary_encode(const std::string& a, std::ostream& dst) {
    uint32_t sz = (uint32_t) a.size();
    dst.write((const char*) &sz, sizeof(uint32_t));
    dst.write((const char*) a.data(), sz);
    return sz + sizeof(uint32_t);
  }

  inline size_t binary_decode(std::istream& src, std::string& dst) {
    uint32_t sz = 0;
    src.read((char*) &sz, sizeof(uint32_t));
    if (sz > 1048576) // sanity (not more that 1 MB)
      return 0;

    dst.resize(sz);
    src.read((char*) dst.data(), sz);
    return src.good() ? sz + sizeof(uint32_t) : 0;
  }

  class binary_codec
  {
  public:
    binary_codec() {}

    template<class T>
    size_t encode(const T& src, std::ostream& dst) {
      return binary_encode(src, dst);
    }

    template<class T>
    inline size_t decode(std::istream& src, T& dst) {
      return binary_decode(src, dst);
    }
  };
};

