#include <boost/uuid/uuid.hpp>
#include <ostream>
#include <istream>
#include <strstream>
#include <vector>
#include <typeinfo>
#pragma once

namespace kspp {
  class binary_serdes
  {
    template<typename T> struct fake_dependency : public std::false_type {};

  public:
    binary_serdes() {}

    static std::string name() { return "kspp::binary"; }

    // does not use schema registry
    template<class T>
    int32_t register_schema(std::string name, const T& dummy){
      return 0;
    }

    template<class T>
    size_t encode(const T& src, std::ostream& dst) {
      static_assert(fake_dependency<T>::value, "you must use specialization to provide a encode for T");
      return 0; // dummy return to quiet warning
    }

    template<class T>
    size_t encode(const std::vector<T>& v, std::ostream& dst) {
      uint32_t vsz = (uint32_t) v.size();
      dst.write((const char*) &vsz, sizeof(uint32_t));
      size_t sz = 4;
      for (auto & i : v)
        sz += kspp::binary_serdes::encode<T>(i, dst);
      return sz;
    }

    template<class T>
    inline size_t decode(const char* payload, size_t size, T& dst) {
      std::istrstream src(payload, size);
      return decode(src, dst);
    }

    template<class T>
    inline size_t decode(const char* payload, size_t size, std::vector<T>& dst) {
      std::istrstream src(payload, size);
      return decode(src, dst);
    }

    template<class T>
    size_t decode(std::istream& src, T& dst) {
      static_assert(fake_dependency<T>::value, "you must use specialization to provide a decode for T");
      return 0; // dummy return to quiet warning
    }

    template<class T>
    inline size_t decode(std::istream& src, std::vector<T>& dst) {
      uint32_t len = 0;
      src.read((char*) &len, sizeof(uint32_t));
      if (len > 1048576) // sanity (not more that 1 MB)
        return 0;
      size_t sz = 4;
      dst.reserve(len);
      for (int i = 0; i != len; ++i) {
        T t;
        sz += kspp::binary_serdes::decode<T>(src, t);
        dst.push_back(t);
      }
      return src.good() ? sz : 0;
    }
  };

  template<> inline size_t binary_serdes::encode(const std::string& src, std::ostream& dst) {
    uint32_t sz = (uint32_t) src.size();
    dst.write((const char*) &sz, sizeof(uint32_t));
    dst.write((const char*) src.data(), sz);
    return sz + sizeof(uint32_t);
  }

  template<> inline size_t binary_serdes::decode(std::istream& src, std::string& dst) {
    uint32_t sz = 0;
    src.read((char*) &sz, sizeof(uint32_t));
    if (sz > 1048576) // sanity (not more that 1 MB)
      return 0;

    dst.resize(sz);
    src.read((char*) dst.data(), sz);
    return src.good() ? sz + sizeof(uint32_t) : 0;
  }

  template<> inline size_t binary_serdes::encode(const int64_t& src, std::ostream& dst) {
    dst.write((const char*) &src, sizeof(std::int64_t));
    return sizeof(std::int64_t);
  }

  template<> inline size_t binary_serdes::decode(std::istream& src, std::int64_t& dst) {
    src.read((char*) &dst, sizeof(std::int64_t));
    return src.good() ? sizeof(std::int64_t) : 0;
  }

  template<> inline size_t binary_serdes::encode(const int32_t& src, std::ostream& dst) {
    dst.write((const char*) &src, sizeof(std::int32_t));
    return sizeof(std::int32_t);
  }

  template<> inline size_t binary_serdes::decode(std::istream& src, std::int32_t& dst) {
    src.read((char*) &dst, sizeof(std::int32_t));
    return src.good() ? sizeof(std::int32_t) : 0;
  }

  template<> inline size_t binary_serdes::encode(const std::uint8_t& src, std::ostream& dst) {
    dst.write((const char*) &src, sizeof(std::uint8_t));
    return sizeof(std::uint8_t);
  }

  template<> inline size_t binary_serdes::decode(std::istream& src, std::uint8_t& dst) {
    src.read((char*) &dst, sizeof(std::uint8_t));
    return src.good() ? sizeof(std::uint8_t) : 0;
  }

  template<> inline size_t binary_serdes::encode(const bool& src, std::ostream& dst) {
    if (src) {
      char one = 0x01;
      dst.write((const char*) &one, 1);
    } else {
      char zero = 0x00;
      dst.write((const char*) &zero, 1);
    }
    return 1;
  }

  template<> inline size_t binary_serdes::decode(std::istream& src, bool& dst) {
    char ch;
    src.read((char*) &ch, 1);
    dst = (ch == 0x00) ? false : true;
    return src.good() ? 1 : 0;
  }

  template<> inline size_t binary_serdes::encode(const boost::uuids::uuid& src, std::ostream& dst) {
    dst.write((const char*) src.data, 16);
    return 16;
  }

  template<> inline size_t binary_serdes::decode(std::istream& src, boost::uuids::uuid& dst) {
    src.read((char*) dst.data, 16);
    return src.good() ? 16 : 0;
  }
}

