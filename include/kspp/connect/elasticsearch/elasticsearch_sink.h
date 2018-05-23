#include <chrono>
#include <kspp/kspp.h>
#include <kspp/utils/async.h>
#include <kspp/utils/http_client.h>
#include <kspp/connect/connection_params.h>
#include <kspp/connect/elasticsearch/elasticsearch_utils.h>
#pragma once

namespace kspp {
  class elasticsearch_sink :
      public kspp::topic_sink<void, kspp::GenericAvro>
  {
    static constexpr const char* PROCESSOR_NAME = "elasticsearch_sink";

  public:
    inline elasticsearch_sink(kspp::topology &,
                              std::string index_name,
                              const kspp::connect::connection_params& cp,
                              std::string id_column,
                              int32_t  http_batch_size,
                              std::chrono::milliseconds http_timeout) :
        kspp::topic_sink<void, kspp::GenericAvro>()
        , _good(true)
        , _closed(false)
        , _start_running(false)
        , _exit(false)
        , _work(std::make_unique<boost::asio::io_service::work>(_ios))
        , _asio_thread([this] { _ios.run(); })
        , _bg([this] { _thread(); })
        , _index_name(index_name)
        , _cp(cp)
        , _id_column(id_column)
        , _http_timeout(http_timeout)
        , _batch_size(http_batch_size)
        , _http_handler(_ios)
        , _next_time_to_send(kspp::milliseconds_since_epoch() + 100)
        , _next_time_to_poll(0)
        , _http_bytes("http_bytes", "bytes")
        , _http_requests("http_request", "msg")
        , _http_timeouts("http_timeout", "msg")
        , _http_error("http_error", "msg")
        , _http_ok("http_ok", "msg") {
      this->add_metric(&_lag);
      this->add_metric(&_http_bytes);
      this->add_metric(&_http_requests);
      this->add_metric(&_http_timeouts);
      this->add_metric(&_http_error);
      this->add_metric(&_http_ok);
      this->add_metrics_tag(KSPP_PROCESSOR_TYPE_TAG, PROCESSOR_NAME);

      curl_global_init(CURL_GLOBAL_NOTHING); /* minimal */
      _http_handler.set_user_agent(
          "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36");

      _start_running = true;
    }

    inline ~elasticsearch_sink() override {
      if (!_closed)
        close();
      _exit = true;
      _http_handler.close();
      _work = nullptr;
      _asio_thread.join();
      _bg.join();
    }

    std::string log_name() const override{
      return PROCESSOR_NAME;
    }

    inline bool eof() const override {
      return ((this->_queue.size() == 0) && (_pending_for_delete.size()==0));
    }

    size_t outbound_queue_len() const override {
      return this->_queue.size();
    }

    inline size_t process(int64_t tick) override{
      size_t sz;
      while (!_pending_for_delete.empty()) {
        ++sz;
        _pending_for_delete.pop_front();
      }
      return sz;
    }

    void close() override {
      if (!_closed){
        _closed = true;
      }
      //TODO??
    }

    inline void flush() override{
      while (!eof()) {
        process(kspp::milliseconds_since_epoch());
        poll(0);
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
      }
      while (true) {
        int ec = 0; // TODO fixme
        //auto ec = _impl.flush(1000);
        if (ec == 0)
          break;
      }
    }

  private:
    void _thread(){
      using namespace std::chrono_literals;

      while (!_exit) {
        if (_closed)
          break;

        // connected
        if (!_start_running) {
          std::this_thread::sleep_for(1s);
          continue;
        }

        if (this->_queue.empty()) {
          std::this_thread::sleep_for(100ms);
          continue;
        }

        //size_t items_to_copy = std::min<size_t>(this->_queue.size(), _batch_size);
        size_t items_to_copy = std::min<size_t>(this->_queue.size(), 1);

        std::string url = _cp.url + "/_bulk";
        //"" + _index_name + "/" + "_doc" + "/" + key_string;
        std::shared_ptr<kspp::http::request> request(new kspp::http::request(kspp::http::POST, url, {"Content-Type: application/x-ndjson"}, _http_timeout));

        if (_cp.user.size() && _cp.password.size())
          request->set_basic_auth(_cp.user, _cp.password);

        size_t msg_in_batch = 0;
        size_t bytes_in_batch = 0;
        event_queue<void, kspp::GenericAvro> in_batch;
        while (!this->_queue.empty() && msg_in_batch < _batch_size) {
          auto msg = this->_queue.pop_and_get(); // this will loose messages at retry... TODO
          //make sure no nulls gets to us
          auto value = msg->record()->value();
          if (value) {
            //{ "index" : { "_index" : "test", "_type" : "_doc", "_id" : "1" } }
            auto key_string = avro2elastic_key_values(*value->valid_schema(), _id_column, *value->generic_datum());
            std::string row0 = "{ \"index\" : { \"_index\" : \"" +  _index_name + "\", \"_type\" : \"_doc\", \"_id\" : \"" +  key_string + "\"  } }\n";
            //LOG(INFO) << row0;
            std::string row1 = avro2elastic_to_json(*value->valid_schema(), *value->generic_datum()) + "\n";
            //LOG(INFO) << row1;
            request->append(row0);
            request->append(row1);
          }
          ++msg_in_batch;
          in_batch.push_back(msg);
        }

        bytes_in_batch += request->tx_content_length();

        //DLOG(INFO) << request->uri();
        //DLOG(INFO) << request->tx_content();

        auto ts0 = kspp::milliseconds_since_epoch();
        // retry sent till we have to exit
        while(!_exit) {
          auto res = _http_handler.perform(request, true);

          // NOT OK?
          if (!res->ok()) {
            if (!res->transport_result()) {
              LOG(ERROR) << "HTTP timeout";
              std::this_thread::sleep_for(10s);
              continue;
            }
            LOG(ERROR) << "HTTP error: " << res->rx_content();
            std::this_thread::sleep_for(10s);
            continue;
          }

          ++(this->_processed_count);
          //this->_lag.add_event_time(tick, trans->event_time());

          // OK...
          auto ts1 = kspp::milliseconds_since_epoch();
          LOG(INFO) << "HTTP call nr of msg: " << msg_in_batch  << ", (" << ts1 - ts0 << ") ms";
          //DLOG_EVERY_N(INFO, 1000) << "influx_sink, http post: " << h->uri() << " got " << h->rx_content_length() << " bytes, time=" << h->milliseconds() << " ms";

          while (!in_batch.empty()) {
            _pending_for_delete.push_back(in_batch.pop_and_get());
          }

          //_msg_cnt += msg_in_batch;
          _http_bytes += bytes_in_batch;
          break;
        }
      } // while (!exit)
      DLOG(INFO) << "exiting thread";
    }

    bool _exit;
    bool _start_running;
    bool _good;
    bool _closed;

    boost::asio::io_service _ios;
    std::unique_ptr<boost::asio::io_service::work> _work;
    std::thread _asio_thread; // internal to http client

    std::thread _bg; // performs the send loop

    event_queue<void, kspp::GenericAvro> _pending_for_delete;

    const kspp::connect::connection_params _cp;
    std::string _index_name;
    std::string _id_column;
    size_t _batch_size;
    std::chrono::milliseconds _http_timeout;

    kspp::http::client _http_handler;
    int64_t _next_time_to_send;
    int64_t _next_time_to_poll;

    kspp::metric_lag _lag;
    kspp::metric_counter _http_requests;
    kspp::metric_counter _http_timeouts;
    kspp::metric_counter _http_error;
    kspp::metric_counter _http_ok;
    kspp::metric_counter _http_bytes;
  };
} // namespace