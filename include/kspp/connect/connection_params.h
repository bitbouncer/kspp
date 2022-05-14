#include <string>
#include <chrono>

#pragma once

namespace kspp {
  namespace connect {

    enum rescrape_policy_t {
      RESCRAPE_OFF, LAST_QUERY_TS, CLIENT_TS
    };

    struct connection_params {

      std::string url;   // where relevant

      std::string host;  // where relevant
      int port;

      //authentication
      std::string user;
      std::string password;

      //resource id database in postgres & tds - not used in elastic
      std::string database_name;

      std::string http_header;

      // drop deletes of never-seen id's
      bool assume_beginning_of_stream = false;
    };

    struct table_params {
      std::chrono::seconds poll_intervall = std::chrono::seconds(60);
      size_t max_items_in_fetch = 30000;
      rescrape_policy_t rescrape_policy = RESCRAPE_OFF;
      uint32_t rescrape_ticks = 1;
      std::string offset_storage;
      std::string query;
      std::string ts_column;
      int ts_multiplier = 1; // normally 1 or 1000 (ie ms or s)
      int ts_utc_offset = 0;
      std::string id_column;
    };
  }
}
