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
//#include <boost/bind.hpp> // TODO replace with std::bind
#pragma once

namespace kspp {
  template<class K, class V>
  class elasticsearch_producer : public generic_producer<K, V> {
  public:
    typedef std::function<std::string(const K &)> key2string_f;
    typedef std::function<std::string(const V &)> value2json_f;

  public:
    enum work_result_t {
      SUCCESS = 0, TIMEOUT = -1, HTTP_ERROR = -2, HTTP_BAD_REQUEST_ERROR = -4
    };

    typedef typename kspp::async::work<elasticsearch_producer::work_result_t>::async_function work_f;


    elasticsearch_producer(std::string remote_write_url, std::string username, std::string password,
                           key2string_f key_conv, value2json_f value_conv, size_t http_batch_size)
        : work_(std::make_shared<boost::asio::io_service::work>(ios_))
          , key_conv_(key_conv)
          , value_conv_(value_conv)
          , http_handler_(ios_, http_batch_size)
          , batch_size_(http_batch_size)
          , http_timeout_(std::chrono::seconds(2))
          , remote_write_url_(remote_write_url)
          , username_(username)
          , password_(password)
          , request_time_("http_request_time", "ms", {0.9, 0.99})
          , timeout_("http_timeout", "msg")
          , http_2xx_("http_request", "msg")
          , http_3xx_("http_request", "msg")
          , http_404_("http_request", "msg")
          , http_4xx_("http_request", "msg")
          , http_5xx_("http_request", "msg")
          , msg_bytes_("bytes_sent", "bytes")
          , bg_([this] { ios_.run(); }), fg_([this] { _process_work(); }) {
      request_time_.add_label(KSPP_DESTINATION_HOST, remote_write_url);
      http_2xx_.add_label("code", "2xx");
      http_3xx_.add_label("code", "3xx");
      http_404_.add_label("code", "404_NO_ERROR");
      http_4xx_.add_label("code", "4xx");
      http_5xx_.add_label("code", "5xx");
      curl_global_init(CURL_GLOBAL_NOTHING); /* minimal */
      http_handler_.set_user_agent(
          "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36");
      connect_async();
    }

    ~elasticsearch_producer() {
      http_handler_.close();
      close();
      work_.reset();
      fg_.join();
      bg_.join();
    }

    inline bool good() const {
      return good_;
    }

    void register_metrics(kspp::processor *parent) override {
      parent->add_metric(&timeout_);
      parent->add_metric(&request_time_);
      parent->add_metric(&http_2xx_);
      parent->add_metric(&http_3xx_);
      parent->add_metric(&http_404_);
      parent->add_metric(&http_4xx_);
      parent->add_metric(&http_5xx_);
      parent->add_metric(&msg_bytes_);
    }

    void close() override {
      closed_ = true;
    }

    bool eof() const override {
      return (incomming_msg_.empty() && done_.empty());
    }

    std::string topic() const override {
      return "unknown-index"; // todo split url?? if we care "http://127.0.0.1:9200/logs/_doc"
    }

    bool is_connected() const {
      return connected_;
    }

    void insert(std::shared_ptr<kevent<K, V>> p) override {
      incomming_msg_.push_back(p);
    }

    void poll() {
      while (!done_.empty()) {
        done_.pop_front();
      }
    }

    size_t queue_size() const override {
      return incomming_msg_.size() + done_.size();
    }

    void set_http_log_interval(int v) {
      http_log_interval_ = v;
    }

    void set_batch_log_interval(int v) {
      batch_log_interval_ = v;
    }

  private:
    void connect_async() {
      connected_ = true; // TODO login and get an auth token
    }

    /*void check_table_exists_async() {
      table_exists_ = true;
      table_create_pending_ = false;
    }
    */

    static void run_work(std::deque<work_f> &work, size_t batch_size) {
      while (work.size()) {
        kspp::async::work<elasticsearch_producer::work_result_t> batch(kspp::async::PARALLEL, kspp::async::ALL);
        size_t nr_of_items_in_batch = std::min<size_t>(work.size(), batch_size);
        for (size_t i = 0; i != nr_of_items_in_batch; ++i) {
          batch.push_back(work[0]);
          work.pop_front();
        }
        batch();
        for (size_t i = 0; i != nr_of_items_in_batch; ++i) {
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

    work_f create_one_http_work(const K &key, const V *value) {
      auto key_string = key_conv_(key);
      std::string url = remote_write_url_ + "/" + key_string;
      kspp::http::method_t request_type = (value) ? kspp::http::PUT : kspp::http::DELETE_;
      std::string body;
      if (value) {
        body = value_conv_(*value);
        //_active_ids.insert(key_string);
      } else {
        /*if (_active_ids.find(key_string)!=_active_ids.end()) {
        _active_ids.erase(key_string);

      } else {
      }
      */
      }
      work_f f = [this, request_type, body, url](std::function<void(work_result_t)> cb) {
        std::vector<std::string> headers({"Content-Type: application/json"});
        auto request = std::make_shared<kspp::http::request>(request_type, url, headers, http_timeout_);

        if (username_.size() && password_.size())
          request->set_basic_auth(username_, password_);

        request->append(body);
        //request->set_trace_level(http::TRACE_LOG_NONE);
        http_handler_.perform_async(
            request,
            [this, cb](std::shared_ptr<kspp::http::request> h) {

              // only observes sucessful roundtrips
              if (h->transport_result())
                request_time_.observe(h->milliseconds());

              //++_http_requests;
              if (!h->ok()) {
                if (!h->transport_result()) {
                  ++timeout_;
                  DLOG(INFO) << "http transport failed - retrying";
                  cb(TIMEOUT);
                  return;
                } else {
                  // if we are deleteing and the document does not exist we do not consider this as a failure
                  if (h->method() == kspp::http::DELETE_ && h->http_result() == kspp::http::not_found) {
                    ++http_404_;
                    cb(SUCCESS);
                    return;
                  }

                  auto ec = h->http_result();
                  if (ec >= 300 && ec < 400)
                    ++http_3xx_;
                  else if (ec >= 400 && ec < 500)
                    ++http_4xx_;
                  else if (ec >= 500 && ec < 600)
                    ++http_5xx_;

                  LOG_FIRST_N(ERROR, 100) << "http " << kspp::http::to_string(h->method()) << ", " << h->uri()
                                          << " HTTPRES = " << h->http_result() << " - retrying, reponse:"
                                          << h->rx_content();
                  LOG_EVERY_N(ERROR, 1000) << "http " << kspp::http::to_string(h->method()) << ", " << h->uri()
                                           << " HTTPRES = " << h->http_result() << " - retrying, reponse:"
                                           << h->rx_content();

                  if (ec == 400) {
                    LOG(ERROR) << "http(400) content: " << h->tx_content();
                    cb(HTTP_BAD_REQUEST_ERROR);
                    return;
                  }

                  cb(HTTP_ERROR);
                  return;
                }
              }
              LOG_EVERY_N(INFO, http_log_interval_) << "http PUT: " << h->uri() << " got " << h->rx_content_length()
                                                    << " bytes, time=" << h->milliseconds() << " ms ("
                                                    << h->rx_kb_per_sec() << " KB/s), #" << google::COUNTER;
              ++http_2xx_;
              msg_bytes_ += h->tx_content_length();
              cb(SUCCESS);
            }); // perform_async
      }; // work
      return f;
    }

    void _process_work() {
      using namespace std::chrono_literals;

      while (!closed_) {
        size_t msg_in_batch = 0;
        event_queue<K, V> in_batch;
        std::deque<work_f> work;
        while (!incomming_msg_.empty() && msg_in_batch < 1000) {
          auto msg = incomming_msg_.front();
          ++msg_in_batch;
          if (auto p = create_one_http_work(msg->record()->key(), msg->record()->value()))
            work.push_back(p);
          in_batch.push_back(msg);
          incomming_msg_.pop_front();
        }

        if (work.size()) {
          auto start = kspp::milliseconds_since_epoch();
          auto ws = work.size();
          run_work(work, batch_size_);
          auto end = kspp::milliseconds_since_epoch();
          LOG_EVERY_N(INFO, batch_log_interval_) << remote_write_url_ << ", worksize: " << ws << ", batch_size: "
                                                 << batch_size_ << ", duration: " << end - start << " ms, #"
                                                 << google::COUNTER;
          while (!in_batch.empty())
            done_.push_back(in_batch.pop_front_and_get());
        } else {
          std::this_thread::sleep_for(2s);
        }
      }
      LOG(INFO) << "worker thread exiting";
    }


    boost::asio::io_service ios_;
    std::shared_ptr<boost::asio::io_service::work> work_;

    key2string_f key_conv_;
    value2json_f value_conv_;

    kspp::http::client http_handler_;
    size_t batch_size_;
    std::chrono::milliseconds http_timeout_;
    const std::string remote_write_url_;
    const std::string username_;
    const std::string password_;

    event_queue<K, V> incomming_msg_;
    event_queue<K, V> done_;

    bool good_ = true;
    bool connected_ = false;
    bool closed_ = false;
/*
    bool table_checked_ = false;
    bool table_exists_ = false;
    bool table_create_pending_ = false;
    bool insert_in_progress_ = false;
*/
    metric_summary request_time_;
    metric_counter timeout_;
    metric_counter http_2xx_;
    metric_counter http_3xx_;
    metric_counter http_404_;
    metric_counter http_4xx_;
    metric_counter http_5xx_;
    metric_counter msg_bytes_;

    //bool _skip_delete_of_non_active;
    std::set<std::string> active_ids_;

    int http_log_interval_ = 1000;
    int batch_log_interval_ = 100;

    std::thread bg_;
    std::thread fg_;
  };
}

