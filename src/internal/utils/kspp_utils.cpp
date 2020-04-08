#include <kspp/utils/kspp_utils.h>
#include <regex>

namespace kspp {
  static bool is_number(const std::string &s) {
    return !s.empty() && std::all_of(s.begin(), s.end(), ::isdigit);
  }

  std::string sanitize_filename(std::string s) {
    auto e = std::regex("[/?<>\\:*|\"]");
    s = std::regex_replace(s, e, "_");
    return s;
  }

  std::vector<int32_t> parse_partition_list(std::string s) {
    std::vector<int32_t> result;
    auto begin = s.find_first_of("[");
    auto end = s.find_first_of("]");
    if (begin == std::string::npos || end == std::string::npos || end - begin < 2)
      return result;
    auto sz = (end - begin) - 1;
    auto s2 = s.substr(begin + 1, sz);
    {
      std::regex rgx("[,\\s+]");
      std::sregex_token_iterator iter(s2.begin(), s2.end(), rgx, -1);
      std::sregex_token_iterator end;
      for (; iter != end; ++iter) {
        if (is_number(*iter))
          result.push_back(stoi(*iter));
      }
    }
    return result;
  }

  std::vector<std::string> parse_string_array(std::string s, std::string regexp) {
    std::vector<std::string> result;
    auto begin = s.find_first_of("[");
    auto end = s.find_first_of("]");
    if (begin == std::string::npos || end == std::string::npos || end - begin < 2)
      return result;
    auto sz = (end - begin) - 1;
    auto s2 = s.substr(begin + 1, sz);
    {
      //std::regex rgx("[,\\s+]");
      //std::regex rgx("[\\,]");
      std::regex rgx(regexp);
      std::sregex_token_iterator iter(s2.begin(), s2.end(), rgx, -1);
      std::sregex_token_iterator end;
      for (; iter != end; ++iter) {
        result.push_back(*iter);
      }
    }
    return result;
  }

  std::vector<int32_t> get_partition_list(int32_t nr_of_partitions) {
    std::vector<int32_t> res;
    for (int32_t i = 0; i != nr_of_partitions; ++i)
      res.push_back(i);
    return res;
  }

  std::string partition_list_to_string(std::vector<int32_t> v) {
    std::string s = "[";
    for (auto i = v.begin(); i != v.end(); ++i)
      s += std::to_string(*i) + ((i != v.end() - 1) ? ", " : "]");
    return s;
  }

  std::string to_string(std::vector<std::string> v) {
    std::string s = "[";
    for (auto i = v.begin(); i != v.end(); ++i)
      s += *i + ((i != v.end() - 1) ? ", " : "]");
    return s;
  }

  std::string to_string(std::set<std::string> v) {
    std::string s = "[";
    size_t count=0;
    for (auto i = v.begin(); i != v.end(); ++i, ++count)
      s += *i + ((count != v.size() - 1) ? ", " : "]");
    return s;
  }

}