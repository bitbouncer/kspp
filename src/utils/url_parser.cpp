#include <kspp/utils/url_parser.h>
#include <regex>

namespace kspp {
  std::vector<url> split_url_list(std::string s, std::string default_scheme) {
    // remove internal whitespaces in s to make sure we do not create empty urls later...
    s.erase(remove_if(s.begin(), s.end(), isspace), s.end());

    std::vector<std::string> splitted_urls;
    std::regex rgx("[,\\s+]");
    std::sregex_token_iterator iter(s.begin(), s.end(), rgx, -1);
    std::sregex_token_iterator end;
    for (; iter != end; ++iter)
      splitted_urls.push_back(*iter);

    std::vector<url> result;
    for (auto str: splitted_urls) {
      url a_url(str, default_scheme);
      if (a_url.good())
        result.push_back(a_url);
    }
    return result;
  }
}
