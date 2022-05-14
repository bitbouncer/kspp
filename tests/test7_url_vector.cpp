#include <iostream>
#include <cassert>
#include <kspp/utils/url_parser.h>

int main(int argc, char **argv) {

  // lets test the trivial case
  {
    std::string s(
        "Https://[2001:db8:85a3:0:0:8a2e:370:7334]:4321/hfadsjkh, https://[2001:db8:85a3:0:0:8a2e:370:7334]:4321/hfadsjkh");
    auto v = kspp::split_url_list(s);
    assert(v.size() == 2);
    for (auto url: v) {
      assert(url.good());
      assert(url.scheme() == "https"); // should be lower case
      assert(url.authority() == "[2001:db8:85a3:0:0:8a2e:370:7334]:4321");
      assert(url.path() == "/hfadsjkh");
      assert(url.str() == "https://[2001:db8:85a3:0:0:8a2e:370:7334]:4321/hfadsjkh");
    }
  }

  // test default scheme
  {
    std::string s(
        "Https://[2001:db8:85a3:0:0:8a2e:370:7334]:4321/hfadsjkh, [2001:db8:85a3:0:0:8a2e:370:7334]:4321/hfadsjkh");
    auto v = kspp::split_url_list(s, "https");
    assert(v.size() == 2);
    for (auto url: v) {
      assert(url.good());
      assert(url.scheme() == "https"); // should be lower case
      assert(url.authority() == "[2001:db8:85a3:0:0:8a2e:370:7334]:4321");
      assert(url.path() == "/hfadsjkh");
      assert(url.str() == "https://[2001:db8:85a3:0:0:8a2e:370:7334]:4321/hfadsjkh");
    }
  }

  return 0;
}

