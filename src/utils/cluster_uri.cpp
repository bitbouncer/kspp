#include <kspp/utils/cluster_uri.h>
#include <regex>
#include <algorithm>

namespace kspp {
  cluster_uri::cluster_uri(std::string s)
      : good_(true) {
    std::string::size_type pos0 = s.find("://");
    if (pos0 == std::string::npos) {
      good_ = false;
      return;
    }
    scheme_ = s.substr(0, pos0);
    std::string::size_type pos1 = s.find('/', pos0 + 3);
    if (pos1 != std::string::npos) {
      authority_ = s.substr(pos0 + 3, pos1 - (pos0 + 3));
      path_ = s.substr(pos1, std::string::npos);
    } else {
      authority_ = s.substr(pos0 + 3, std::string::npos);
    }

    // remove internal whitespaces in authority_
    authority_.erase(remove_if(authority_.begin(), authority_.end(), isspace), authority_.end());

    std::transform(scheme_.begin(), scheme_.end(), scheme_.begin(), ::tolower);
  }

  cluster_uri::cluster_uri(std::string s, std::string default_scheme)
      : good_(true) {
    std::string::size_type pos0 = s.find("://");
    if (pos0 == std::string::npos) {
      scheme_ = default_scheme;
      std::string::size_type pos1 = s.find('/', 0);
      if (pos1 != std::string::npos) {
        authority_ = s.substr(0, pos1);
        path_ = s.substr(pos1, std::string::npos);
      } else {
        authority_ = s.substr(0, std::string::npos);
      }
    } else {
      scheme_ = s.substr(0, pos0);

      std::string::size_type pos1 = s.find('/', pos0 + 3);
      if (pos1 != std::string::npos) {
        authority_ = s.substr(pos0 + 3, pos1 - (pos0 + 3));
        path_ = s.substr(pos1, std::string::npos);
      } else {
        authority_ = s.substr(pos0 + 3, std::string::npos);
      }
    }
    // remove internal whitespaces in authority_
    authority_.erase(remove_if(authority_.begin(), authority_.end(), isspace), authority_.end());
    std::transform(scheme_.begin(), scheme_.end(), scheme_.begin(), ::tolower);
  }

  std::vector<std::string> cluster_uri::split_authority() const {
    std::vector<std::string> result;
    std::regex rgx("[,\\s+]");
    std::sregex_token_iterator iter(authority_.begin(), authority_.end(), rgx, -1);
    std::sregex_token_iterator end;
    for (; iter != end; ++iter)
      result.push_back(*iter);
    return result;
  }
}