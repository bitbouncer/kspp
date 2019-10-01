#include <kspp/utils/url.h>

#pragma once

namespace kspp {
  std::vector<kspp::url> split_url_list(std::string s, std::string default_scheme = "");
}
