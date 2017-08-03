
#pragma once
namespace csi {
namespace http {
enum status_type
{
  undefined = 0,

  //1xx informational
  continue_with_request = 100,
  switching_protocols = 101,
  processing = 102,
  checkpoint = 103,
  uri_to_long = 122,

  //2xx success
  ok = 200,
  created = 201,
  accepted = 202,
  processed_non_authorative = 203,
  no_content = 204,
  reset_content = 205,
  partial_content = 206,
  multi_status = 207,
  already_reported = 208,
  im_used = 226,

  //3xx redirection
  multiple_choices = 300,
  moved_permanently = 301,
  moved_temporarily = 302,
  see_other = 303, // ??? bad name....
  not_modified = 304,
  use_proxy = 305,
  switch_proxy = 306,
  temporary_redirect = 307,
  resume_incomplete = 308,

  //4xx client error
  bad_request = 400,
  unauthorized = 401,
  forbidden = 403,
  not_found = 404,
  precondition_failed = 412,

  //5xx server error
  internal_server_error = 500,
  not_implemented = 501,
  bad_gateway = 502,
  service_unavailable = 503
};

/*
static std::string to_string(status_type s)
{
switch (s)
{
case ok:                        return "ok";
case created:                   return "created";
case accepted:                  return "accepted";
case processed_non_authorative
case csi::http::no_content:                return "no_content";
case csi::http::multiple_choices:          return "multiple_choices";
case csi::http::moved_permanently:         return "moved_permanently";
case csi::http::moved_temporarily:         return "moved_temporarily";
case csi::http::not_modified:              return "not_modified";
case csi::http::bad_request:               return "bad_request";
case csi::http::unauthorized:              return "unauthorized";
case csi::http::forbidden:                 return "forbidden";
case csi::http::not_found:                 return "not_found";
case csi::http::precondition_failed:       return "precondition_failed";
case csi::http::internal_server_error:     return "internal_server_error";
case csi::http::not_implemented:           return "not_implemented";
case csi::http::bad_gateway:               return "bad_gateway";
case csi::http::service_unavailable:       return "service_unavailable";
default:                                   return boost::lexical_cast<std::string>((long)s);
};
};
*/

  // must be equal to the one in http_parser.h
enum method_t
{
  DELETE_ = 0, /* without underscore clashed with macro */
  GET = 1,
  HEAD = 2,
  POST = 3,
  PUT = 4,
  /* pathological */
  CONNECT = 5,
  OPTIONS = 6,
  TRACE = 7,
  /* webdav */
  COPY = 8,
  LOCK = 9,
  MKCOL = 10,
  MOVE = 11,
  PROPFIND = 12,
  PROPPATCH = 13,
  SEARCH = 14,
  UNLOCK = 15,
  /* subversion */
  REPORT = 16,
  MKACTIVITY = 17,
  CHECKOUT = 18,
  MERGE = 19,
  /* upnp */
  MSEARCH = 20,
  NOTIFY = 21,
  SUBSCRIBE = 22,
  UNSUBSCRIBE = 23,
  /* RFC-5789 */
  PATCH = 24,
  PURGE = 25
};

inline const std::string& to_string(csi::http::method_t e) {
  static const std::string table[csi::http::PURGE + 1]
  {
      "DELETE",
          "GET",
          "HEAD",
          "POST",
          "PUT",
          /* pathological */
          "CONNECT",
          "OPTIONS",
          "TRACE",
          /* webdav */
          "COPY",
          "LOCK",
          "MKCOL",
          "MOVE",
          "PROPFIND",
          "PROPPATCH",
          "SEARCH",
          "UNLOCK",
          /* subversion */
          "REPORT",
          "MKACTIVITY",
          "CHECKOUT",
          "MERGE",
          /* upnp */
          "MSEARCH",
          "NOTIFY",
          "SUBSCRIBE",
          "UNSUBSCRIBE",
          /* RFC-5789 */
          "PATCH",
          "PURGE"
  };
  return table[e];
}

struct header_t
{
  header_t() {}
  header_t(const std::string& n, const std::string& v) : name(n), value(v) {}
  std::string name;
  std::string value;
};
} // namespace
} // namespace
