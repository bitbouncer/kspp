#include <string>
#pragma once

namespace kspp {
  namespace connect {

    enum connect_ts_policy_t { GREATER_OR_EQUAL, GREATER };

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

      connect_ts_policy_t connect_ts_policy = GREATER_OR_EQUAL;
    };
  }
}
