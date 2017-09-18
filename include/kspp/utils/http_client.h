//
// http_client.h
// ~~~~~~~~~~
// Copyright 2014 Svante Karlsson CSI AB (svante.karlsson at csi dot se)
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)
//

#include <chrono>
#include <future>
#include <atomic>
#include <thread>
#include <mutex>
#include <sstream>
#include <curl/curl.h>
#include <boost/asio.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/function.hpp>
#include <boost/thread/condition_variable.hpp>
#include <boost/algorithm/string/predicate.hpp>
#include <boost/algorithm/string.hpp>
#include <kspp/utils/spinlock.h>
#include <glog/logging.h>
#pragma once

namespace kspp {
  namespace http {
    enum status_type {
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

    // must be equal to the one in http_parser.h
    enum method_t {
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

    const std::string &to_string(kspp::http::method_t e);

    struct header_t {
      header_t() {}

      header_t(const std::string &n, const std::string &v) : name(n), value(v) {}

      std::string name;
      std::string value;
    };


    class client;

    // dummy implementetation
    class buffer {
    public:
      buffer() { _data.reserve(32 * 1024); }

      void reserve(size_t sz) { _data.reserve(sz); }

      void append(const uint8_t *p, size_t sz) { _data.insert(_data.end(), p, p + sz); }

      void append(uint8_t s) { _data.push_back(s); }

      const uint8_t *data() const { return &_data[0]; }

      size_t size() const { return _data.size(); }

      void pop_back() { _data.resize(_data.size() - 1); }

      void clear() { _data.clear(); }

    private:
      std::vector<uint8_t> _data;
    };

    class request {
      friend class kspp::http::client;

    public:
      typedef boost::function<void(std::shared_ptr<request>)> callback;

      request(kspp::http::method_t method,
              const std::string &uri,
              const std::vector<std::string> &headers,
              std::chrono::milliseconds timeout=std::chrono::seconds(2));

      ~request();

    public:
      /*
       * ca_cert_path path to pem encoded file
       */
      void set_ca_cert_path(std::string ca_cert_path);

      /*
       * client_cert_path path to pem encoded file
       * client_key_path path to pem encoded file
       * client_key_passphrase
       */
      void set_client_credentials(std::string client_cert_path, std::string client_key_path, std::string client_key_passphrase);

      /*
       * user_define_id user defined string that is prepended in logs if defined
       */
      void set_request_id(std::string user_define_id);

      /*
       * turns on detailed logging
       */
      void set_verbose(bool state);


      void set_tx_headers(const std::vector<std::string> &headers);


      void set_timeout(const std::chrono::milliseconds &timeout);


      inline int64_t milliseconds() const {
        std::chrono::milliseconds duration = std::chrono::duration_cast<std::chrono::milliseconds>(_end_ts - _start_ts);
        return duration.count();
      }

      inline int64_t microseconds() const {
        std::chrono::microseconds duration = std::chrono::duration_cast<std::chrono::microseconds>(_end_ts - _start_ts);
        return duration.count();
      }

      inline void append(const std::string &s) {
        _tx_stream << s;
      }

      inline std::string tx_content() const {
        return _tx_stream.str();
      }

      inline const char *rx_content() const {
        return _rx_buffer.size() ? (const char *) _rx_buffer.data() : "";
      }

      inline size_t tx_content_length() const {
        return _tx_stream.str().size();
      }

      inline size_t rx_content_length() const {
        return _rx_buffer.size();
      }

      inline int rx_kb_per_sec() const {
        auto sz = rx_content_length();
        int64_t ms = milliseconds();
        return (int) (ms == 0 ? 0 : sz / ms);
      }

      inline const std::string &uri() const {
        return _uri;
      }

      inline kspp::http::status_type http_result() const {
        return _http_result;
      }

      inline bool transport_result() const {
        return _transport_ok;
      }

      inline bool ok() const {
        return _transport_ok && (_http_result >= 200) && (_http_result < 300);
      }

      std::string get_rx_header(const std::string &header) const;

    private:

      void curl_start(std::shared_ptr<request> self);

      void curl_stop();

      static std::shared_ptr<request> lookup(CURL *e);

      kspp::http::method_t _method;
      std::string _uri;
      std::vector<std::string> _tx_headers;
      std::vector<kspp::http::header_t> _rx_headers;
      std::chrono::steady_clock::time_point _start_ts;
      std::chrono::steady_clock::time_point _end_ts;
      std::chrono::milliseconds _timeout;

      //SSL stuff
      std::string _ca_cert;
      std::string _client_cert;
      std::string _client_key;
      std::string _client_key_passphrase;

      // logging stuff
      std::string _request_id;
      bool _curl_verbose;

      callback _callback;
      //TX
      std::stringstream _tx_stream; // temporary for test...
      //RX
      buffer _rx_buffer;

      kspp::http::status_type _http_result;
      bool _transport_ok;

      //curl stuff
      CURL *_curl_easy;
      curl_slist *_curl_headerlist;
      bool _curl_done;
      std::shared_ptr<request> _this; // used to keep object alive when only curl knows about the context
    };

    class client {
    public:
      client(boost::asio::io_service &io_service);
      ~client();

      void set_user_agent(std::string s);

      void close();
      bool done();
      void perform_async(std::shared_ptr<kspp::http::request> request, kspp::http::request::callback cb);
      std::shared_ptr<kspp::http::request> perform(std::shared_ptr<kspp::http::request> request, bool verbose);

    protected:
      void _perform(std::shared_ptr<kspp::http::request> request);

      // must not be called within curl callbacks - post a asio message instead
      void _poll_remove(std::shared_ptr<request> p);

      // CURL CALLBACKS
      static curl_socket_t _opensocket_cb(void *clientp, curlsocktype purpose, struct curl_sockaddr *address);
      static int _sock_cb(CURL *e, curl_socket_t s, int what, void *user_data, void *per_socket_user_data);
      static int _multi_timer_cb(CURLM *multi, long timeout_ms, void *userp);
      static int _closesocket_cb(void *user_data, curl_socket_t item);

      curl_socket_t opensocket_cb(curlsocktype purpose, struct curl_sockaddr *address);

      int sock_cb(CURL *e, curl_socket_t s, int what, void *per_socket_user_data);

      //BOOST EVENTS
      void socket_rx_cb(const boost::system::error_code &ec,
                        boost::asio::ip::tcp::socket *tcp_socket,
                        std::shared_ptr<request> context);

      void socket_tx_cb(const boost::system::error_code &ec,
                        boost::asio::ip::tcp::socket *tcp_socket,
                        std::shared_ptr<request> context);

      void timer_cb(const boost::system::error_code &ec);

      int multi_timer_cb(CURLM *multi, long timeout_ms);

      int closesocket_cb(curl_socket_t item);

      void check_completed();

      boost::asio::io_service &_io_service;
      mutable spinlock _spinlock;
      boost::asio::steady_timer _timer;
      std::map<curl_socket_t, boost::asio::ip::tcp::socket *> _socket_map;
      CURLM *_multi;
      int _curl_handles_still_running;
      std::string _user_agent_header;
      bool _closing;
    };
  } // namespace
} // namespace
