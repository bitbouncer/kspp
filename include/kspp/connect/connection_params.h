#include <string>
#pragma once

namespace kspp {
  namespace connect {
    struct connection_params {
      std::string host;
      int port;
      std::string user;
      std::string password;
      std::string database;
    };
  }
}
