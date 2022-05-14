#include <iostream>
#include <cassert>
#include <kspp/utils/cluster_uri.h>

int main(int argc, char **argv) {

  // lets test the trivial case
  {
    kspp::cluster_uri uri("Zk://127.0.0.1:2181,192.168.100.44:2181/nisseGul");
    assert(uri.good());
    assert(uri.scheme() == "zk"); // should be lower case
    assert(uri.authority() == "127.0.0.1:2181,192.168.100.44:2181");
    assert(uri.path() == "/nisseGul");
    assert(uri.str() == "zk://127.0.0.1:2181,192.168.100.44:2181/nisseGul");
  }

  {
    kspp::cluster_uri uri("Zk://127.0.0.1:2181,192.168.100.44:2181");
    assert(uri.good());
    assert(uri.scheme() == "zk"); // should be lower case
    assert(uri.authority() == "127.0.0.1:2181,192.168.100.44:2181");
    assert(uri.path() == "");
    assert(uri.str() == "zk://127.0.0.1:2181,192.168.100.44:2181");
  }

  //ip v6....
  //https://[2001:db8:85a3:0:0:8a2e:370:7334]:4321,[::1]:5555,[0:0:0:0:0:0:0:1]:1432
  {
    kspp::cluster_uri uri("https://[2001:db8:85a3:0:0:8a2e:370:7334]:4321,[::1]:5555,[0:0:0:0:0:0:0:1]:1432");
    assert(uri.good());
    assert(uri.scheme() == "https");
    assert(uri.authority() == "[2001:db8:85a3:0:0:8a2e:370:7334]:4321,[::1]:5555,[0:0:0:0:0:0:0:1]:1432");
    assert(uri.path() == "");
    assert(uri.str() == "https://[2001:db8:85a3:0:0:8a2e:370:7334]:4321,[::1]:5555,[0:0:0:0:0:0:0:1]:1432");
  }

  // with some whitespace
  {
    kspp::cluster_uri uri("https://[2001:db8:85a3:0:0:8a2e:370:7334]:4321, [::1]:5555 ,[0:0:0:0:0:0:0:1]:1432");
    assert(uri.good());
    assert(uri.scheme() == "https");
    assert(uri.authority() == "[2001:db8:85a3:0:0:8a2e:370:7334]:4321,[::1]:5555,[0:0:0:0:0:0:0:1]:1432");
    assert(uri.path() == "");
    assert(uri.str() == "https://[2001:db8:85a3:0:0:8a2e:370:7334]:4321,[::1]:5555,[0:0:0:0:0:0:0:1]:1432");
  }

  //https://[::1]:5555
  //http://[2001:0db8:85a3:0000:0000:8a2e:0370:7334]:1234
  //http://[0:0:0:0:0:0:0:1]:1432 (edited)

  return 0;
}

