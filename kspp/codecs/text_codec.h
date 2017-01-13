#include <string>
#include <ostream>
#include <istream>
#include <boost/uuid/uuid.hpp>
#pragma once

namespace kspp {
  class text_codec
  {
  public:
    text_codec() {}

    static std::string name() { return "text"; }

    template<class T>
    size_t encode(const T& src, std::ostream& dst) {
      std::string s = typeid(T).name();
      dst << s;
      return s.size();
    }

    template<class T>
    size_t decode(std::istream& src, T& dst) {
      return 0;
      //return decode(this, src, dst);
    }
  };

  template<> inline size_t text_codec::encode(const std::string& src, std::ostream& dst) {
    dst << src;
    return src.size();
  }

  template<> inline size_t text_codec::decode(std::istream& src, std::string& dst) {
    dst.clear();
    std::getline(src, dst);
    return dst.size();
  }

  template<> inline size_t text_codec::encode(const bool& src, std::ostream& dst) {
    dst << (src ? "true" : "false");
    return src ? 4 : 5;
  }

  template<> inline size_t text_codec::decode(std::istream& src, bool& dst) {
    std::string s;
    std::getline(src, s);
    dst = (s == "true") ? true : false;
    return s.size();
  }

  template<> inline size_t text_codec::encode(const int& src, std::ostream& dst) {
    auto s = std::to_string(src);
    dst << s;
    return s.size();
  }
   
  template<> inline size_t text_codec::encode(const int64_t& src, std::ostream& dst) {
    auto s = std::to_string(src);
    dst << s;
    return s.size();
  }

  template<> inline size_t text_codec::encode(const uint64_t& src, std::ostream& dst) {
    auto s = std::to_string(src);
    dst << s;
    return s.size();
  }
     
  template<> inline size_t text_codec::decode(std::istream& src, int& dst) {
    std::string s;
    std::getline(src, s);
    dst = std::atoi(s.c_str());
    return s.size();
  }
};

