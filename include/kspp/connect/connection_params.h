#include <string>
#pragma once

namespace kspp {
  namespace connect {
    struct connection_params {

      std::string url;   // where relevant

      std::string host;  // where relevant
      int port;

      //authentication
      std::string user;
      std::string password;

      //resource id
      std::string database;

      std::string http_header; // where relevant;
    };
  }
}
