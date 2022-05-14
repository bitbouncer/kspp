#include <kspp/utils/http_client.h>
#include <glog/logging.h>

using namespace std::chrono_literals;

namespace kspp {
  namespace http {

/*#define CURLEASY_THROW_NOT_OK(res) \
 if (res!=CURLE_OK) {          \
   throw std::invalid_argument(curl_easy_strerror(res)); \
   }
*/

    inline void CURLEASY_THROW_NOT_OK(CURLcode res) {
      if (res != CURLE_OK)
        throw std::invalid_argument(curl_easy_strerror(res));
    }

    inline void CURLMULTI_THROW_NOT_OK(CURLMcode res) {
      if (res != CURLM_OK)
        throw std::invalid_argument(curl_multi_strerror(res));
    }


/*#define CURLMULTI_THROW_NOT_OK(res) \
 if (res!=CURLM_OK) {          \
   throw std::invalid_argument(curl_multi_strerror(res)); \
   }
*/

    const std::string &to_string(kspp::http::method_t e) {
      static const std::string table[kspp::http::PURGE + 1]
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


    // static helpers
    /*static size_t write_callback_std_stream(void *ptr, size_t size, size_t nmemb, std::ostream *stream) {
      size_t sz = size * nmemb;
      //BOOST_LOG_TRIVIAL(trace) << BOOST_CURRENT_FUNCTION << ", in size: " << sz;
      stream->write((char *) ptr, sz);
      return sz;
    }
    */

    static size_t write_callback_buffer(void *ptr, size_t size, size_t nmemb, kspp::http::buffer *buf) {
      size_t sz = size * nmemb;
      //BOOST_LOG_TRIVIAL(trace) << BOOST_CURRENT_FUNCTION << ", in size: " << sz;
      buf->append((const uint8_t *) ptr, sz);
      return sz;
    }

    /*static size_t read_callback_std_stream(void *ptr, size_t size, size_t nmemb, std::istream *stream) {
      size_t max_sz = size * nmemb;
      stream->read((char *) ptr, max_sz);
      size_t actual = stream->gcount();
      //BOOST_LOG_TRIVIAL(trace) << BOOST_CURRENT_FUNCTION << ", out size: " << actual;
      return actual;
    }*/

    static size_t parse_headers(void *buffer, size_t size, size_t nmemb, std::vector<kspp::http::header_t> *v) {
      size_t sz = size * nmemb;;
      char *begin = (char *) buffer;
      if (v) {
        char *separator = (char *) memchr(begin, ':', sz);
        char *newline = (char *) memchr(begin, '\r', sz); // end of value
        if (separator && newline) {
          //since get_rx_header() is case insensitive - no need to transform here
          //std::transform(begin, separator, begin, ::tolower);
          char *value_begin = separator + 1;
          while (isspace(*value_begin)) value_begin++; // leading white spaces
          //size_t datalen = newline - value_begin;
          v->emplace_back(kspp::http::header_t(std::string(begin, separator), std::string(value_begin, newline)));
        }
      }
      return sz;
    }

    static int my_trace(CURL *,
                        curl_infotype type,
                        char *data,
                        size_t sz,
                        void *userp) {
      std::string *request_id = (std::string *) userp; //
      //const char *text;
      //(void)handle; /* prevent compiler warning */

      switch (type) {
        case CURLINFO_TEXT: {
          std::string s(data);
          if (request_id->size())
            LOG(INFO) << *request_id << ", " << s;
          else
            LOG(INFO) << s;

          return 0;
        }
          //LOG(INFO) << "curl: " << std::string(data);
          //fprintf(stderr, "== Info: %s", data);
          /* FALLTHROUGH */
        default: /* in case a new one is introduced to shock us */
          return 0;

          /*
             case CURLINFO_HEADER_OUT:
              text = "=> Send header";
              break;
            case CURLINFO_DATA_OUT:
              text = "=> Send data";
              break;
            case CURLINFO_SSL_DATA_OUT:
              text = "=> Send SSL data";
              break;
            case CURLINFO_HEADER_IN:
              text = "<= Recv header";
              break;
            case CURLINFO_DATA_IN:
              text = "<= Recv data";
              break;
            case CURLINFO_SSL_DATA_IN:
              text = "<= Recv SSL data";
              break;
            */
      }

      //DLOG(INFO) << text;
      //dump(text, stderr, (unsigned char *)data, size, config->trace_ascii);
      return 0;
    }


    request::request(kspp::http::method_t method,
                     const std::string &uri,
                     const std::vector<std::string> &headers,
                     std::chrono::milliseconds timeout
    )
        : _method(method)
        , _uri(uri)
        , _tx_headers(headers)
        , _timeout(timeout)
        , _http_result(kspp::http::undefined)
        , _transport_ok(true)
        , _curl_easy(NULL)
        , _curl_headerlist(NULL)
        , _curl_done(false) {
      _curl_easy = curl_easy_init();
    }

    request::~request() {
      if (_curl_easy)
        curl_easy_cleanup(_curl_easy);
      _curl_easy = NULL;
      if (_curl_headerlist)
        curl_slist_free_all(_curl_headerlist);
    }

    void request::set_request_id(std::string user_defined) {
      _request_id = user_defined;
    }

    void request::set_basic_auth(const std::string &user, const std::string &password) {
      CURLEASY_THROW_NOT_OK(curl_easy_setopt(_curl_easy, CURLOPT_HTTPAUTH, (long) CURLAUTH_BASIC));
      CURLEASY_THROW_NOT_OK(curl_easy_setopt(_curl_easy, CURLOPT_USERNAME, user.c_str()));
      CURLEASY_THROW_NOT_OK(curl_easy_setopt(_curl_easy, CURLOPT_PASSWORD, password.c_str()));
    }

    void request::set_ca_cert_path(std::string path) {
      _ca_cert = path;
    }

    void request::set_client_credentials(std::string client_cert_path, std::string client_key_path,
                                         std::string client_key_passphrase) {
      _client_cert = client_cert_path;
      _client_key = client_key_path;
      _client_key_passphrase = client_key_passphrase;
    }

    void request::set_verify_host(bool state) {
      _verify_host = state;
    }


    void request::set_tx_headers(const std::vector<std::string> &headers) {
      _tx_headers = headers;
    }

    void request::set_timeout(const std::chrono::milliseconds &timeout) {
      _timeout = timeout;
    }

    void request::set_trace_level(trace_log_level level) {
      _log_level = level;
    }

    void request::curl_start(std::shared_ptr<request> self) {
      // lets clear state if this is a restarted transfer...
      // TBD how do we handle post and put? what about ingested data??
      if (_curl_headerlist)
        curl_slist_free_all(_curl_headerlist);
      _curl_headerlist = NULL;

      _curl_done = false;
      _transport_ok = true;
      _http_result = kspp::http::undefined;

      _rx_headers.clear();
      _rx_buffer.clear();

      _this = self;
      curl_easy_setopt(_curl_easy, CURLOPT_PRIVATE, this);
    }

    void request::curl_stop() {
      curl_easy_setopt(_curl_easy, CURLOPT_PRIVATE, NULL);
      _this.reset();
    }

    std::shared_ptr<request> request::lookup(CURL *e) {
      request *_this = NULL;
      curl_easy_getinfo(e, CURLINFO_PRIVATE, &_this);
      assert(_this);
      return _this->_this;
    }


    std::string request::get_rx_header(const std::string &header) const {
      for (std::vector<kspp::http::header_t>::const_iterator i = _rx_headers.begin(); i != _rx_headers.end(); ++i) {
        if (boost::iequals(header, i->name))
          return i->value;
      }
      return "";
    }

    client::client(boost::asio::io_service &io_service, size_t max_connection_cache)
        : io_service_(io_service)
        , timer_(io_service_)
        , user_agent_header_("User-Agent:csi-http/0.1")
        , closing_(false) {
      multi_ = curl_multi_init();
      curl_multi_setopt(multi_, CURLMOPT_SOCKETFUNCTION, _sock_cb);
      curl_multi_setopt(multi_, CURLMOPT_SOCKETDATA, this);
      curl_multi_setopt(multi_, CURLMOPT_TIMERFUNCTION, _multi_timer_cb);
      curl_multi_setopt(multi_, CURLMOPT_TIMERDATA, this);
      curl_multi_setopt(multi_, CURLMOPT_MAXCONNECTS, (long) max_connection_cache);
    }

    client::~client() {
      close();
      // wait for the close to be done
      while (multi_)
        std::this_thread::sleep_for(std::chrono::milliseconds(100)); // patch for not waiting for http to die... FIXME
    }

    void client::set_user_agent(std::string s) {
      user_agent_header_ = std::string("User-Agent:") + s;
    }

    void client::close() {
      if (closing_)
        return;
      closing_ = true;
      if (multi_) {
        io_service_.post([this]() {
          curl_multi_cleanup(multi_);
          multi_ = NULL;
        });
      }
      timer_.cancel();
    }

    bool client::done() {
      return (curl_handles_still_running_ == 0);
    }

    void client::perform_async(std::shared_ptr<kspp::http::request> request, kspp::http::request::callback cb) {
      request->_callback = cb;
      io_service_.post([this, request]() {
        _perform(request);
      });
    }

    std::shared_ptr<kspp::http::request> client::perform(std::shared_ptr<kspp::http::request> request) {
      std::promise<std::shared_ptr<kspp::http::request>> p;
      std::future<std::shared_ptr<kspp::http::request>> f = p.get_future();
      perform_async(request, [&p](std::shared_ptr<kspp::http::request> result) {
        p.set_value(result);
      });
      f.wait();
      return f.get();
    }

    void client::_perform(std::shared_ptr<kspp::http::request> request) {
      try {
        request->curl_start(request); // increments usage count and keeps object around until curl thinks its done.
        CURLEASY_THROW_NOT_OK(curl_easy_setopt(request->_curl_easy, CURLOPT_DEBUGFUNCTION,
                                               my_trace)); // not active if not using CURLOPT_VERBOSE==1
        CURLEASY_THROW_NOT_OK(curl_easy_setopt(request->_curl_easy, CURLOPT_DEBUGDATA, &request->_request_id));
        CURLEASY_THROW_NOT_OK(
            curl_easy_setopt(request->_curl_easy, CURLOPT_OPENSOCKETFUNCTION, &client::_opensocket_cb));
        CURLEASY_THROW_NOT_OK(curl_easy_setopt(request->_curl_easy, CURLOPT_OPENSOCKETDATA, this));
        CURLEASY_THROW_NOT_OK(
            curl_easy_setopt(request->_curl_easy, CURLOPT_CLOSESOCKETFUNCTION, &client::_closesocket_cb));
        CURLEASY_THROW_NOT_OK(curl_easy_setopt(request->_curl_easy, CURLOPT_CLOSESOCKETDATA, this));
        CURLEASY_THROW_NOT_OK(curl_easy_setopt(request->_curl_easy, CURLOPT_NOSIGNAL,
                                               1L)); // try to avoid signals to timeout address resolution calls
        CURLEASY_THROW_NOT_OK(curl_easy_setopt(request->_curl_easy, CURLOPT_URL, request->_uri.c_str()));

        // should we should skip the ssl stuff below if the url does not begin with https?
        // it does not seem to hurt to init ssl on a http connection

        //SSL OPTIONS

        if (request->_ca_cert.size() == 0) {
          CURLEASY_THROW_NOT_OK(curl_easy_setopt(request->_curl_easy, CURLOPT_SSL_VERIFYPEER, 0L));   // unsafe
          CURLEASY_THROW_NOT_OK(curl_easy_setopt(request->_curl_easy, CURLOPT_SSL_VERIFYHOST, 0L));  // unsafe
          //res = curl_easy_setopt(request->_curl_easy, CURLOPT_VERBOSE, 1L);
        } else {
          CURLEASY_THROW_NOT_OK(curl_easy_setopt(request->_curl_easy, CURLOPT_SSL_VERIFYPEER, 1L));
          if (request->_verify_host) {
            CURLEASY_THROW_NOT_OK(curl_easy_setopt(request->_curl_easy, CURLOPT_SSL_VERIFYHOST, 2L));
          } else {
            CURLEASY_THROW_NOT_OK(curl_easy_setopt(request->_curl_easy, CURLOPT_SSL_VERIFYHOST, 0L));  // unsafe
          }
          CURLEASY_THROW_NOT_OK(curl_easy_setopt(request->_curl_easy, CURLOPT_CAINFO, request->_ca_cert.c_str()));
        }

        if (request->_client_cert.size() > 0) {
          CURLEASY_THROW_NOT_OK(curl_easy_setopt(request->_curl_easy, CURLOPT_SSLCERT, request->_client_cert.c_str()));
          CURLEASY_THROW_NOT_OK(curl_easy_setopt(request->_curl_easy, CURLOPT_SSLKEY, request->_client_key.c_str()));
          CURLEASY_THROW_NOT_OK(
              curl_easy_setopt(request->_curl_easy, CURLOPT_SSLKEYPASSWD, request->_client_key_passphrase.c_str()));
          CURLEASY_THROW_NOT_OK(curl_easy_setopt(request->_curl_easy, CURLOPT_SSLKEYTYPE, "PEM"));
        }

        // retrieve cert info
        //curl_easy_setopt(_curl, CURLOPT_CERTINFO, 1);

        if (request->_log_level == TRACE_LOG_VERBOSE)
          CURLEASY_THROW_NOT_OK(curl_easy_setopt(request->_curl_easy, CURLOPT_VERBOSE, 1L));

        switch (request->_method) {
          case kspp::http::GET:
            CURLEASY_THROW_NOT_OK(curl_easy_setopt(request->_curl_easy, CURLOPT_HTTPGET, 1));
            //curl_easy_setopt(request->_curl_easy, CURLOPT_FOLLOWLOCATION, 1L);
            break;

          case kspp::http::PUT:
            /* only works with read callbacks
             * res = curl_easy_setopt(request->_curl_easy, CURLOPT_UPLOAD, 1L);
            * res = curl_easy_setopt(request->_curl_easy, CURLOPT_INFILESIZE_LARGE, (curl_off_t) request->tx_content_length());
            */
            // this seems to be the right way with PUT and strings
            CURLEASY_THROW_NOT_OK(curl_easy_setopt(request->_curl_easy, CURLOPT_CUSTOMREQUEST, "PUT")); /* !!! */
            CURLEASY_THROW_NOT_OK(
                curl_easy_setopt(request->_curl_easy, CURLOPT_POSTFIELDS, request->tx_content().data()));
            CURLEASY_THROW_NOT_OK(
                curl_easy_setopt(request->_curl_easy, CURLOPT_POSTFIELDSIZE, (long) request->tx_content_length()));
            break;

          case kspp::http::POST: {
            //res = curl_easy_setopt(request->_curl_easy, CURLOPT_POST, 1);
            CURLEASY_THROW_NOT_OK(curl_easy_setopt(request->_curl_easy, CURLOPT_BUFFERSIZE, 102400L));
            CURLEASY_THROW_NOT_OK(curl_easy_setopt(request->_curl_easy, CURLOPT_CUSTOMREQUEST, "POST"));
            // must be different in post and put???
            //res = curl_easy_setopt(request->_curl_easy, CURLOPT_POSTFIELDS, NULL);
            CURLEASY_THROW_NOT_OK(
                curl_easy_setopt(request->_curl_easy, CURLOPT_POSTFIELDS, request->tx_content().data()));
            size_t sz = request->tx_content_length();
            //LOG(INFO) << "sending - content lenght " << sz;
            CURLEASY_THROW_NOT_OK(curl_easy_setopt(request->_curl_easy, CURLOPT_POSTFIELDSIZE, (long) sz));
          }
            break;

          case kspp::http::DELETE_:
            CURLEASY_THROW_NOT_OK(curl_easy_setopt(request->_curl_easy, CURLOPT_CUSTOMREQUEST, "DELETE"));
            break;

          default:
            LOG(FATAL) << "unsupported method: " << request->_method;
        };

        /* the request */
        //res = curl_easy_setopt(request->_curl_easy, CURLOPT_READFUNCTION, read_callback_std_stream);
        //res = curl_easy_setopt(request->_curl_easy, CURLOPT_READDATA, &request->_tx_stream);

        /* the reply */
        CURLEASY_THROW_NOT_OK(curl_easy_setopt(request->_curl_easy, CURLOPT_WRITEFUNCTION, write_callback_buffer));
        CURLEASY_THROW_NOT_OK(curl_easy_setopt(request->_curl_easy, CURLOPT_WRITEDATA, &request->_rx_buffer));

        CURLEASY_THROW_NOT_OK(curl_easy_setopt(request->_curl_easy, CURLOPT_LOW_SPEED_TIME, 3L));
        CURLEASY_THROW_NOT_OK(curl_easy_setopt(request->_curl_easy, CURLOPT_LOW_SPEED_LIMIT, 10L));

        // if this is a resend we have to clear this before using it again
        if (request->_curl_headerlist) {
          //assert(false); // is this really nessessary??
          curl_slist_free_all(request->_curl_headerlist);
          request->_curl_headerlist = NULL;
        }

        static const char buf[] = "Expect:";
        /* initalize custom header list (stating that Expect: 100-continue is not wanted */
        request->_curl_headerlist = curl_slist_append(request->_curl_headerlist, buf);
        request->_curl_headerlist = curl_slist_append(request->_curl_headerlist, user_agent_header_.c_str());
        for (std::vector<std::string>::const_iterator i = request->_tx_headers.begin();
             i != request->_tx_headers.end(); ++i)
          request->_curl_headerlist = curl_slist_append(request->_curl_headerlist, i->c_str());
        CURLEASY_THROW_NOT_OK(curl_easy_setopt(request->_curl_easy, CURLOPT_HTTPHEADER, request->_curl_headerlist));
        CURLEASY_THROW_NOT_OK(curl_easy_setopt(request->_curl_easy, CURLOPT_TIMEOUT_MS, request->_timeout.count()));
        CURLEASY_THROW_NOT_OK(curl_easy_setopt(request->_curl_easy, CURLOPT_HEADERDATA, &request->_rx_headers));
        CURLEASY_THROW_NOT_OK(curl_easy_setopt(request->_curl_easy, CURLOPT_HEADERFUNCTION, parse_headers));
        CURLEASY_THROW_NOT_OK(curl_easy_setopt(request->_curl_easy, CURLOPT_FOLLOWLOCATION, 1L));
        //CURLEASY_THROW_NOT_OK(curl_easy_setopt(request->_curl_easy, CURLOPT_IPRESOLVE, CURL_IPRESOLVE_V4));

        request->_start_ts = std::chrono::steady_clock::now();

//DEBUF ONLY
/*        if (request->_method == http::GET) {
        DLOG(INFO) << "http_client: "
                   << to_string(request->_method)
                   << ", uri: "
                   << request->_uri;
      } else {
        DLOG(INFO) << "http_client: "
                   << to_string(request->_method)
                   << ", uri: "
                   << request->_uri
                   << ", content_length: "
                   << request->tx_content_length();
      }
*/

        CURLMULTI_THROW_NOT_OK(curl_multi_add_handle(multi_, request->_curl_easy));
      } catch (std::exception &e) {
        LOG(ERROR) << "http request failed: " << e.what();
      }
    }

    // must not be called within curl callbacks - post a asio message instead
    void client::_poll_remove(std::shared_ptr<request> p) {
      //BOOST_LOG_TRIVIAL(trace) << this << ", " << BOOST_CURRENT_FUNCTION << ", handle: " << p->_curl_easy;
      curl_multi_remove_handle(multi_, p->_curl_easy);
    }

    // CURL CALLBACKS
    curl_socket_t client::_opensocket_cb(void *clientp, curlsocktype purpose, struct curl_sockaddr *address) {
      return ((client *) clientp)->opensocket_cb(purpose, address);
    }

    curl_socket_t client::opensocket_cb(curlsocktype purpose, struct curl_sockaddr *address) {
      /* IPV4 */
      if (purpose == CURLSOCKTYPE_IPCXN && address->family == AF_INET) {
        /* create a tcp socket object */
        boost::asio::ip::tcp::socket *tcp_socket = new boost::asio::ip::tcp::socket(io_service_);

        /* open it and get the native handle*/
        boost::system::error_code ec;
        tcp_socket->open(boost::asio::ip::tcp::v4(), ec);

        if (ec) {
          LOG(ERROR) << this << ", " << ", open failed, socket: " << ec << ", (" << ec.message() << ")";
          delete tcp_socket;
          return CURL_SOCKET_BAD;
        }

        curl_socket_t sockfd = tcp_socket->native_handle();
        /* save it for monitoring */
        {
          spinlock::scoped_lock xxx(spinlock_);
          socket_map_.insert(std::pair<curl_socket_t, boost::asio::ip::tcp::socket *>(sockfd, tcp_socket));
        }
        //BOOST_LOG_TRIVIAL(trace) << this << ", " << BOOST_CURRENT_FUNCTION << " open ok, socket: " << sockfd;
        return sockfd;
      }
      // IPV6
      if (purpose == CURLSOCKTYPE_IPCXN && address->family == AF_INET6) {
        /* create a tcp socket object */
        boost::asio::ip::tcp::socket *tcp_socket = new boost::asio::ip::tcp::socket(io_service_);

        /* open it and get the native handle*/
        boost::system::error_code ec;
        tcp_socket->open(boost::asio::ip::tcp::v6(), ec);
        if (ec) {
          LOG(ERROR) << this << ", open failed, socket: " << ec << ", (" << ec.message() << ")";
          delete tcp_socket;
          return CURL_SOCKET_BAD;
        }

        curl_socket_t sockfd = tcp_socket->native_handle();
        /* save it for monitoring */
        spinlock::scoped_lock xxx(spinlock_);
        {
          socket_map_.insert(std::pair<curl_socket_t, boost::asio::ip::tcp::socket *>(sockfd, tcp_socket));
        }
        //LOG(INFO) << ",  open ok, socket: " << sockfd;
        return sockfd;
      }

      LOG(ERROR) << "http_client::opensocket_cb unsupported address family";
      return CURL_SOCKET_BAD;
    }

    int client::_sock_cb(CURL *e, curl_socket_t s, int what, void *user_data, void *per_socket_user_data) {
      return ((client *) user_data)->sock_cb(e, s, what, per_socket_user_data);
    }

    int client::sock_cb(CURL *e, curl_socket_t s, int what, void *per_socket_user_data) {
      if (what == CURL_POLL_REMOVE) {
        //LOG(INFO) << ", CURL_POLL_REMOVE, socket: " << s;
        return 0;
      }

      boost::asio::ip::tcp::socket *tcp_socket = (boost::asio::ip::tcp::socket *) per_socket_user_data;
      if (!tcp_socket) {
        //we try to find the data in our own mapping
        //if we find it - register this to curl so we dont have to do this every time.
        spinlock::scoped_lock xxx(spinlock_);
        {
          std::map<curl_socket_t, boost::asio::ip::tcp::socket *>::iterator it = socket_map_.find(s);
          if (it != socket_map_.end()) {
            tcp_socket = it->second;
            curl_multi_assign(multi_, s, tcp_socket);
          }
        }

        if (!tcp_socket) {
          //LOG(INFO) << "socket: " << s  << " is a c-ares socket, ignoring";
          return 0;
        }
      }

      std::shared_ptr<request> context = request::lookup(e);

      switch (what) {
        case CURL_POLL_IN:
          //LOG(INFO) << ", CURL_POLL_IN, socket: " << s;
          tcp_socket->async_read_some(boost::asio::null_buffers(),
                                      [this, tcp_socket, context](const boost::system::error_code &ec,
                                                                  std::size_t bytes_transferred) {
                                        socket_rx_cb(ec, tcp_socket, context);
                                      });
          break;
        case CURL_POLL_OUT:
          //LOG(INFO) << ", CURL_POLL_OUT, socket: " << s;
          tcp_socket->async_write_some(boost::asio::null_buffers(),
                                       [this, tcp_socket, context](const boost::system::error_code &ec,
                                                                   std::size_t bytes_transferred) {
                                         socket_tx_cb(ec, tcp_socket, context);
                                       });
          break;
        case CURL_POLL_INOUT:
          //LOG(INFO) << ", CURL_POLL_INOUT, socket: " << s;
          tcp_socket->async_read_some(boost::asio::null_buffers(),
                                      [this, tcp_socket, context](const boost::system::error_code &ec,
                                                                  std::size_t bytes_transferred) {
                                        socket_rx_cb(ec, tcp_socket, context);
                                      });
          tcp_socket->async_write_some(boost::asio::null_buffers(),
                                       [this, tcp_socket, context](const boost::system::error_code &ec,
                                                                   std::size_t bytes_transferred) {
                                         socket_tx_cb(ec, tcp_socket, context);
                                       });
          break;
        case CURL_POLL_REMOVE:
          // should never happen - handled above
          break;
      };
      return 0;
    }

    //BOOST EVENTS
    void client::socket_rx_cb(const boost::system::error_code &ec, boost::asio::ip::tcp::socket *tcp_socket,
                              std::shared_ptr<request> context) {
      if (!ec && !context->_curl_done) {
        try {
          CURLMULTI_THROW_NOT_OK(curl_multi_socket_action(multi_, tcp_socket->native_handle(), CURL_CSELECT_IN,
                                                          &curl_handles_still_running_));
        } catch (std::exception &e) {
          LOG(ERROR) << "socket_rx_cb failed: " << e.what();
        }
        if (!context->_curl_done)
          tcp_socket->async_read_some(boost::asio::null_buffers(),
                                      [this, tcp_socket, context](const boost::system::error_code &ec,
                                                                  std::size_t bytes_transferred) {
                                        socket_rx_cb(ec, tcp_socket, context);
                                      });
        check_completed();
      }
    }

    void client::socket_tx_cb(const boost::system::error_code &ec, boost::asio::ip::tcp::socket *tcp_socket,
                              std::shared_ptr<request> context) {
      if (!ec) {
        try {
          CURLMULTI_THROW_NOT_OK(curl_multi_socket_action(multi_, tcp_socket->native_handle(), CURL_CSELECT_OUT,
                                                          &curl_handles_still_running_));
        } catch (std::exception &e) {
          LOG(ERROR) << "socket_tx_cb failed: " << e.what();
        }
        if (!context->_curl_done)
          tcp_socket->async_write_some(boost::asio::null_buffers(),
                                       [this, tcp_socket, context](const boost::system::error_code &ec,
                                                                   std::size_t bytes_transferred) {
                                         socket_tx_cb(ec, tcp_socket, context);
                                       });
        check_completed();
      }
    }

    void client::timer_cb(const boost::system::error_code &ec) {
      if (!ec) {
        // CURL_SOCKET_TIMEOUT, 0 is corrent on timeouts http://curl.haxx.se/libcurl/c/curl_multi_socket_action.html
        try {
          CURLMULTI_THROW_NOT_OK(
              curl_multi_socket_action(multi_, CURL_SOCKET_TIMEOUT, 0, &curl_handles_still_running_));
        } catch (std::exception &e) {
          LOG(ERROR) << "timer_cb failed: " << e.what();
        }
        check_completed();
        //check_multi_info(); //TBD kolla om denna ska vara här
      }
    }

    //void keepalivetimer_cb(const boost::system::error_code & error);
    //void _asio_closesocket_cb(curl_socket_t item);

    //curl callbacks
    int client::_multi_timer_cb(CURLM *multi, long timeout_ms, void *userp) {
      return ((client *) userp)->multi_timer_cb(multi, timeout_ms);
    }

    int client::multi_timer_cb(CURLM *multi, long timeout_ms) {
      /* cancel running timer */
      timer_.cancel();

      if (timeout_ms > 0) {
        if (!closing_) {
          timer_.expires_from_now(std::chrono::milliseconds(timeout_ms));
          timer_.async_wait([this](const boost::system::error_code &ec) {
            timer_cb(ec);
          });
        }
      } else {
        /* call timeout function immediately */
        boost::system::error_code error; /*success*/
        timer_cb(error);
      }
      check_completed(); // ska den vara här ???
      return 0;
    }

    //static size_t         _write_cb(void *ptr, size_t size, size_t nmemb, void *data);

    int client::_closesocket_cb(void *user_data, curl_socket_t item) {
      return ((client *) user_data)->closesocket_cb(item);
    }

    int client::closesocket_cb(curl_socket_t item) {
      spinlock::scoped_lock xxx(spinlock_);
      {
        std::map<curl_socket_t, boost::asio::ip::tcp::socket *>::iterator it = socket_map_.find(item);
        if (it != socket_map_.end()) {
          boost::system::error_code ec;
          it->second->cancel(ec);
          it->second->close(ec);
          boost::asio::ip::tcp::socket *s = it->second;

          //curl_multi_assign(_multi, it->first, NULL); // we need to remove this at once since curl likes to reuse sockets
          socket_map_.erase(it);
          io_service_.post([this, s]() {
            delete s; // must be deleted after operations on socket completed. therefore the _io_service.post
          });
        }
      }
      return 0;
    }

    void client::check_completed() {
      /* call curl_multi_perform or curl_multi_socket_action first, then loop
      through and check if there are any transfers that have completed */

      CURLMsg *m = NULL;
      do {
        int msgq = 0;
        m = curl_multi_info_read(multi_, &msgq);
        if (m && (m->msg == CURLMSG_DONE)) {
          CURL *e = m->easy_handle;


          std::shared_ptr<request> context = request::lookup(e);

          long http_result = 0;
          CURLcode curl_res = curl_easy_getinfo(e, CURLINFO_RESPONSE_CODE, &http_result);
          if (curl_res == CURLE_OK)
            context->_http_result = (kspp::http::status_type) http_result;
          else
            context->_http_result = (kspp::http::status_type) 0;

          context->_end_ts = std::chrono::steady_clock::now();
          context->_curl_done = true;
          context->_transport_ok = (http_result > 0);

          /*
           * if (context->_request_id.size()>0) {
            DLOG(INFO) << context->_request_id << ", " << to_string(context->_method)
                       << ", uri: " << context->_uri
                       << ", res: " << http_result
                       << ", content_length: "
                       << ((context->_method == http::GET) ? context->rx_content_length() : context->tx_content_length())
                       << ", time: " << context->milliseconds() << " ms";
          } else {
            DLOG(INFO) << to_string(context->_method)
                       << ", uri: " << context->_uri
                       << ", res: " << http_result
                       << ", content_length: "
                       << ((context->_method == http::GET) ? context->rx_content_length() : context->tx_content_length())
                       << ", time: " << context->milliseconds() << " ms";
          }
           */

          if (context->_transport_ok) {
            std::string content_length_str = context->get_rx_header("Content-Length");
            if (content_length_str.size()) {
              if ((int) context->rx_content_length() < atoi(content_length_str.c_str())) {
                context->_transport_ok = false;
                DLOG(WARNING) << "content length header not matching actual content - failing request";
              }
            }
          }

          if (context->_callback) {
            // lets make sure the character in the buffer after the content is NULL
            // this is convinient to make parsingfast without copying to string...
            // rapidxml/rapidjson et al...
            context->_rx_buffer.append(0);
            context->_rx_buffer.pop_back(); // don't change the size..

            context->_callback(context);
          }

          curl_multi_remove_handle(multi_, context->_curl_easy);
          context->curl_stop(); // must be the last one...
        }
      } while (m);
    }
  } // namespace
} // namespace
