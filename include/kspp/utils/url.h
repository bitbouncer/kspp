#include <string>
#include <vector>

#pragma once

/**

 * Consider http://host1.domain.com:2110/23987462
 *
 * The url is broker down in into its parts: scheme ("http"), authority (ie. host and port),
 * path ("/foo/bar"),
 * The scheme is lower-cased.
 */

namespace kspp {
  class url {
  public:
    //accepts a url without a scheme if given a non empty default_scheme
    url(std::string s, std::string default_scheme = "");

    bool good() const { return good_; }

    std::string scheme() const { return scheme_; }

    std::string authority() const { return authority_; }

    std::string path() const { return path_; }

    std::string str() const { return scheme_ + "://" + authority_ + path_; }

  private:
    bool good_;
    std::string scheme_;
    std::string authority_;
    std::string path_;
  };
}
