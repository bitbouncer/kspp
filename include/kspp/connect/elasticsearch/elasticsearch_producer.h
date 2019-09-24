#include <chrono>
#include <memory>
#include <kspp/kspp.h>
#include <kspp/internal/queue.h>
#include <kspp/utils/async.h>
#include <kspp/utils/http_client.h>
#include <kspp/avro/generic_avro.h>
#include <kspp/internal/event_queue.h>
#include <kspp/kevent.h>
#include <kspp/connect/generic_producer.h>
#include <kspp/connect/connection_params.h>
#include <kspp/metrics/metrics.h>
#include <boost/bind.hpp> // TODO replace with std::bind
#pragma once

namespace kspp {
  template<class K, class V>
  class elasticsearch_producer : public generic_producer<kspp::generic_avro, kspp::generic_avro>
  {
  public:
    typedef std::function<std::string(const K&)> key2string_f;
    typedef std::function<std::string(const V&)> value2json_f;

  public:
    enum work_result_t { SUCCESS = 0, TIMEOUT = -1, HTTP_ERROR = -2, HTTP_BAD_REQUEST_ERROR = -4};

    typedef typename kspp::async::work<elasticsearch_producer::work_result_t>::async_function work_f;


    elasticsearch_producer(const kspp::connect::connection_params& cp, key2string_f key_conv, value2json_f value_conv, size_t http_batch_size)
    : _work(new boost::asio::io_service::work(_ios))
        , _fg([this] { _process_work(); })
        , _bg(boost::bind(&boost::asio::io_service::run, &_ios))
        , _key_conv(key_conv)
        , _value_conv(value_conv)
        , _http_handler(_ios, http_batch_size)
        , _cp(cp)
        , _batch_size(http_batch_size)
        , _http_timeout(std::chrono::seconds(2))
        , _good(true)
        , _closed(false)
        , _connected(false)
        , _table_checked(false)
        , _table_exists(false)
        , _table_create_pending(false)
        , _insert_in_progress(false)
        , _skip_delete_of_non_active(cp.assume_beginning_of_stream)
        , _request_time("http_request_time", "ms", { 0.9, 0.99 })
        , _timeout("http_timeout", "msg")
        , _http_2xx("http_request", "msg")
        , _http_3xx("http_request", "msg")
        , _http_4xx("http_request", "msg")
        , _http_404("http_request", "msg")
        , _http_5xx("http_request", "msg")
        , _msg_bytes("bytes_sent", "bytes"){
      _request_time.add_label(KSPP_DESTINATION_HOST, _cp.host);
      _http_2xx.add_label("code", "2xx");
      _http_3xx.add_label("code", "3xx");
      _http_404.add_label("code", "404_NO_ERROR");
      _http_4xx.add_label("code", "4xx");
      _http_5xx.add_label("code", "5xx");
      curl_global_init(CURL_GLOBAL_NOTHING); /* minimal */
      _http_handler.set_user_agent("Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36");
      connect_async();
    }

    ~elasticsearch_producer(){
      _http_handler.close();
      close();
      _work.reset();
      _fg.join();
      _bg.join();
    }

    bool good() const {
      return _good;
    }

    void register_metrics(kspp::processor* parent) override{
      parent->add_metric(&_timeout);
      parent->add_metric(&_request_time);
      parent->add_metric(&_http_2xx);
      parent->add_metric(&_http_3xx);
      parent->add_metric(&_http_404);
      parent->add_metric(&_http_4xx);
      parent->add_metric(&_http_5xx);
      parent->add_metric(&_msg_bytes);
    }

    void close() override{
      _closed=true;
    }

    bool eof() const override {
      return (_incomming_msg.empty() && _done.empty());
    }

    std::string topic() const override {
      return _cp.database_name;
    }

    //void stop();

    bool is_connected() const {
      return _connected;
    }

    void insert(std::shared_ptr<kevent<K, V>> p) override{
      _incomming_msg.push_back(p);
    }

    void poll(){
      while (!_done.empty()) {
        _done.pop_front();
      }
    }

    size_t queue_size() const override {
      return _incomming_msg.size() + _done.size();
    }

  private:
    void connect_async(){
      _connected = true; // TODO login and get an auth token
    }

    void check_table_exists_async(){
      _table_exists = true;
      _table_create_pending = false;
    }

    static void run_work(std::deque<work_f>& work, size_t batch_size) {
      while (work.size()) {
        kspp::async::work<elasticsearch_producer::work_result_t> batch(kspp::async::PARALLEL, kspp::async::ALL);
        size_t nr_of_items_in_batch = std::min<size_t>(work.size(), batch_size);
        for (int i = 0; i != nr_of_items_in_batch; ++i) {
          batch.push_back(work[0]);
          work.pop_front();
        }
        batch();
        for (int i = 0; i != nr_of_items_in_batch; ++i) {
          switch (batch.get_result(i)) {
            case elasticsearch_producer::SUCCESS:
              break;
            case elasticsearch_producer::TIMEOUT:
              //if (++timeouts < max_timeouts_per_batch)
              work.push_front(batch.get_function(i));
              break;
            case elasticsearch_producer::HTTP_ERROR:
              // therre are a number of es codes that means it's overloaded - we should back off Todo
              work.push_front(batch.get_function(i));
              break;
            case elasticsearch_producer::HTTP_BAD_REQUEST_ERROR:
              // nothing to do - no point of retrying this
              break;
          }
        }
      }
    }

    work_f create_one_http_work(const K& key, const V* value){
      //auto key_string = avro_2_raw_column_value(*key.generic_datum());
      auto key_string = _key_conv(key);
      std::string url = _cp.url + "/" + _cp.database_name + "/" + "_doc" + "/" + key_string;

      kspp::http::method_t request_type = (value) ? kspp::http::PUT : kspp::http::DELETE_;

      std::string body;

      if (value) {
        //body = avro2elastic_json(*value->valid_schema(), *value->generic_datum());
        body = _value_conv(*value);
        _active_ids.insert(key_string);
      } else {
        if (_active_ids.find(key_string)!=_active_ids.end()) {
          _active_ids.erase(key_string);
        } else {
        }
      }
        work_f f = [this, request_type, body, url](std::function<void(work_result_t)> cb) {
        std::vector<std::string> headers({ "Content-Type: application/json" });
        auto request = std::make_shared<kspp::http::request>(request_type, url, headers, _http_timeout);

        if (_cp.user.size() && _cp.password.size())
          request->set_basic_auth(_cp.user, _cp.password);

        request->append(body);
        //request->set_trace_level(http::TRACE_LOG_NONE);
        _http_handler.perform_async(
            request,
            [this, cb](std::shared_ptr<kspp::http::request> h) {

              // only observes sucessful roundtrips
              if (h->transport_result())
                _request_time.observe(h->milliseconds());

              //++_http_requests;
              if (!h->ok()) {
                if (!h->transport_result()) {
                  ++_timeout;
                  DLOG(INFO) << "http transport failed - retrying";
                  cb(TIMEOUT);
                  return;
                } else {
                  // if we are deleteing and the document does not exist we do not consideer this as a failure
                  if (h->method()==kspp::http::DELETE_ && h->http_result()==kspp::http::not_found){
                    ++_http_404;
                    cb(SUCCESS);
                    return;
                  }

                  auto ec = h->http_result();
                  if (ec>=300 && ec <400)
                    ++_http_3xx;
                  else if (ec>=400 && ec <500)
                    ++_http_4xx;
                  else if (ec>=500 && ec <600)
                    ++_http_5xx;

                  LOG(ERROR) << "http " << kspp::http::to_string(h->method()) << ", "  << h->uri() << " HTTPRES = " << h->http_result() << " - retrying, reponse:" << h->rx_content();

                  if (ec==400) {
                    LOG(ERROR) << "http(400) content: " << h->tx_content();
                    cb(HTTP_BAD_REQUEST_ERROR);
                    return;
                  }

                  cb(HTTP_ERROR);
                  return;
                }
              }
              LOG_EVERY_N(INFO, 1000) << "http PUT: " << h->uri() << " got " << h->rx_content_length() << " bytes, time="
                                      << h->milliseconds() << " ms (" << h->rx_kb_per_sec() << " KB/s), #"
                                      << google::COUNTER;
              ++_http_2xx;
              _msg_bytes += h->tx_content_length();
              cb(SUCCESS);
            }); // perform_async
      }; // work
      return f;
    }

    void _process_work(){
      using namespace std::chrono_literals;

      while (!_closed) {
        size_t msg_in_batch = 0 ;
        event_queue<K, V> in_batch;
        std::deque<work_f> work;
        while(!_incomming_msg.empty() && msg_in_batch<1000) {
          auto msg = _incomming_msg.front();
          ++msg_in_batch;
          if (auto p = create_one_http_work(msg->record()->key(), msg->record()->value()))
            work.push_back(p);
          in_batch.push_back(msg);
          _incomming_msg.pop_front();
        }

        if (work.size()) {
          auto start = kspp::milliseconds_since_epoch();
          auto ws = work.size();
          //LOG(INFO) << "run_work...: ";

          run_work(work, _batch_size);
          auto end = kspp::milliseconds_since_epoch();
          LOG(INFO) << _cp.url << ", worksize: " << ws << ", batch_size: " << _batch_size << ", duration: " << end - start << " ms";

          while (!in_batch.empty())
            _done.push_back(in_batch.pop_front_and_get());

        } else {
          std::this_thread::sleep_for(2s);
        }
      }
      LOG(INFO) << "worker thread exiting";
    }


    key2string_f _key_conv;
    value2json_f _value_conv;

    boost::asio::io_service _ios;
    std::shared_ptr<boost::asio::io_service::work> _work;

    std::thread _bg;
    kspp::http::client _http_handler;
    size_t _batch_size;
    std::chrono::milliseconds _http_timeout;

    const kspp::connect::connection_params _cp;
    const std::string _id_column;

    event_queue<kspp::generic_avro, kspp::generic_avro> _incomming_msg;
    event_queue<kspp::generic_avro, kspp::generic_avro> _done;

    bool _good;
    bool _connected;
    bool _closed;
    bool _table_checked;
    bool _table_exists;
    bool _table_create_pending;
    bool _insert_in_progress;

    std::thread _fg;
    metric_counter _timeout;
    metric_counter _http_2xx;
    metric_counter _http_3xx;
    metric_counter _http_404;
    metric_counter _http_4xx;
    metric_counter _http_5xx;
    metric_counter _msg_bytes;
    metric_summary _request_time;

    bool _skip_delete_of_non_active;
    std::set<std::string> _active_ids;
  };
}

