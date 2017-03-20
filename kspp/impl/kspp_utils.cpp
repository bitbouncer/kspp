#include <kspp/kspp.h>
#include <regex>
#include <string>

namespace kspp {
static bool is_number(const std::string &s) {
  return !s.empty() && std::all_of(s.begin(), s.end(), ::isdigit);
}

std::string sanitize_filename(std::string s) {
  auto e = std::regex("[/?<>\\:*|\"]");
  s = std::regex_replace(s, e, "_");
  return s;
}

std::vector<int> parse_partition_list(std::string s) {
  std::vector<int> result;
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

std::vector<int> get_partition_list(int32_t nr_of_partitions) {
  std::vector<int> res;
  for (int32_t i = 0; i != nr_of_partitions; ++i)
    res.push_back(i);
  return res;
}
}