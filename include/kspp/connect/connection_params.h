#include <string>
#include <chrono>
#pragma once

namespace kspp {
  namespace connect {

    //enum row_constness_t { MUTABLE, IMMUTABLE };

    //enum connect_ts_policy_t { GREATER_OR_EQUAL, GREATER };

    enum rescrape_policy_t { RESCRAPE_OFF, LAST_QUERY_TS, CLIENT_TS };

    struct connection_params {

      std::string url;   // where relevant

      std::string host;  // where relevant
      int port;

      //authentication
      std::string user;
      std::string password;

      //resource id
      std::string database;

      std::string http_header;
    };

    struct table_params {
      std::chrono::seconds poll_intervall = std::chrono::seconds(60);
      //connect_ts_policy_t connect_ts_policy = GREATER_OR_EQUAL;
      //row_constness_t row_constness = MUTABLE;
      size_t max_items_in_fetch=30000;
      rescrape_policy_t rescrape_policy = RESCRAPE_OFF;
      uint32_t rescrape_ticks = 1;
    };
    }
}

/*inline std::string to_string(kspp::connect::row_constness_t c) {
  switch (c) {
    case kspp::connect::MUTABLE: return "MUTABLE";
    case kspp::connect::IMMUTABLE: return "IMMUTABLE";
    default:
      return "UNKNOWN";
  }
}
 */

/*
 * inline std::string to_string(kspp::connect::connect_ts_policy_t c){
  switch (c) {
    case kspp::connect::GREATER_OR_EQUAL: return "GREATER_OR_EQUAL";
    case kspp::connect::GREATER: return "GREATER";
    default:
      return "UNKNOWN";
  }
}
 */

