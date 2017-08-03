#include "http_defs.h"
#pragma once

namespace csi {
namespace http {
class client;

 // dummy implementetation
class buffer
{
  public:
  buffer() { _data.reserve(32 * 1024); }
  void reserve(size_t sz) { _data.reserve(sz); }
  void append(const uint8_t* p, size_t sz) { _data.insert(_data.end(), p, p + sz); }
  void append(uint8_t s) { _data.push_back(s); }
  const uint8_t* data() const { return &_data[0]; }
  size_t size() const { return _data.size(); }
  void pop_back() { _data.resize(_data.size() - 1); }
  void clear() { _data.clear(); }

  private:
  std::vector<uint8_t> _data;
};

class request
{
  friend class csi::http::client;
  public:
  typedef boost::function <void(std::shared_ptr<request>)> callback;

  request(csi::http::method_t method, const std::string& uri, const std::vector<std::string>& headers, const std::chrono::milliseconds& timeout, bool verbose = false) :
    _transport_ok(true),
    _method(method),
    _uri(uri),
    _curl_verbose(verbose),
    _timeoutX(timeout),
    _http_result(csi::http::undefined),
    _tx_headers(headers),
    _tx_stream(std::ios_base::ate | std::ios_base::in | std::ios_base::out),
    _curl_easy(NULL),
    _curl_headerlist(NULL),
    _curl_done(false) {
    _curl_easy = curl_easy_init();
  }

  ~request() {
    if (_curl_easy)
      curl_easy_cleanup(_curl_easy);
    _curl_easy = NULL;
    if (_curl_headerlist)
      curl_slist_free_all(_curl_headerlist);
  }

  public:
  inline void curl_start(std::shared_ptr<request> self) {
    // lets clear state if this is a restarted transfer...
    // TBD how do we handle post and put? what about ingested data??
    if (_curl_headerlist)
      curl_slist_free_all(_curl_headerlist);
    _curl_headerlist = NULL;

    _curl_done = false;
    _transport_ok = true;
    _http_result = csi::http::undefined;

    _rx_headers.clear();
    _rx_buffer.clear();

    _this = self;
    curl_easy_setopt(_curl_easy, CURLOPT_PRIVATE, this);
  }

  inline void curl_stop() {
    curl_easy_setopt(_curl_easy, CURLOPT_PRIVATE, NULL);
    _this.reset();
  }

  static std::shared_ptr<request> lookup(CURL* e) {
    request* _this = NULL;
    curl_easy_getinfo(e, CURLINFO_PRIVATE, &_this);
    assert(_this);
    return _this->_this;
  }

  //inline call_context::handle curl_handle() { return _this; }

  inline int64_t milliseconds() const { std::chrono::milliseconds duration = std::chrono::duration_cast<std::chrono::milliseconds>(_end_ts - _start_ts); return duration.count(); }
  inline int64_t microseconds() const { std::chrono::microseconds duration = std::chrono::duration_cast<std::chrono::microseconds>(_end_ts - _start_ts); return duration.count(); }
  void append(const std::string& s) { _tx_stream << s; }
  std::string tx_content() const { return _tx_stream.str(); }
  const char* rx_content() const { return _rx_buffer.size() ? (const char*) _rx_buffer.data() : ""; }
  inline size_t tx_content_length() const { return _tx_stream.str().size(); }
  inline size_t rx_content_length() const { return _rx_buffer.size(); }
  inline int    rx_kb_per_sec() const { auto sz = rx_content_length(); int64_t ms = milliseconds();  return (int) (ms == 0 ? 0 : sz / ms); }
  inline void set_verbose(bool state) { _curl_verbose = state; }
  inline const std::string& uri() const { return _uri; }
  inline csi::http::status_type http_result() const { return _http_result; }
  inline bool transport_result() const { return _transport_ok; }
  inline bool ok() const { return _transport_ok && (_http_result >= 200) && (_http_result < 300); }

  std::string get_rx_header(const std::string& header) const {
    for (std::vector<csi::http::header_t>::const_iterator i = _rx_headers.begin(); i != _rx_headers.end(); ++i) {
      if (boost::iequals(header, i->name))
        return i->value;
    }
    return "";
  }

  private:
  csi::http::method_t                   _method;
  std::string                           _uri;
  std::vector<std::string>              _tx_headers;
  std::vector<csi::http::header_t>      _rx_headers;
  std::chrono::steady_clock::time_point _start_ts;
  std::chrono::steady_clock::time_point _end_ts;
  std::chrono::milliseconds             _timeoutX;
  callback                              _callback;

  //TX
  std::stringstream                     _tx_stream; // temporary for test...
  //RX
  buffer                                _rx_buffer;

  csi::http::status_type                _http_result;
  bool                                  _transport_ok;

  //curl stuff
  CURL*                                 _curl_easy;
  curl_slist*                           _curl_headerlist;
  bool                                  _curl_done;
  std::shared_ptr<request>              _this; // used to keep object alive when only curl knows about the context
  bool                                  _curl_verbose;
};

inline std::shared_ptr<request> create_http_request(csi::http::method_t method, const std::string& uri, const std::vector<std::string>& headers, const std::chrono::milliseconds& timeout, bool verbose = false) {
  return std::make_shared<request>(method, uri, headers, timeout, verbose);

}
} // namespace
} // namespace