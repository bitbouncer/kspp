#include <kspp/connect/elasticsearch/elasticsearch_producer.h>
#include <kspp/connect/elasticsearch/elasticsearch_utils.h>
#include <glog/logging.h>
#include <boost/bind.hpp>
namespace kspp {
  using namespace std::chrono_literals;

  elasticsearch_producer::elasticsearch_producer(std::string index_name,
                                                 const kspp::connect::connection_params& cp,
                                                 std::string id_column,
                                                 size_t batch_size)
    : _work(new boost::asio::io_service::work(_ios))
    , _fg([this] { _process_work(); })
    , _bg(boost::bind(&boost::asio::io_service::run, &_ios))
    , _http_handler(_ios, batch_size)
    , _index_name(index_name)
    , _cp(cp)
    , _id_column(id_column)
    , _batch_size(batch_size)
    , _http_timeout(std::chrono::seconds(2))
    , _msg_cnt(0)
    , _msg_bytes(0)
    , _good(true)
    , _closed(false)
    , _connected(false)
    , _table_checked(false)
    , _table_exists(false)
    , _table_create_pending(false)
    , _insert_in_progress(false) {
    curl_global_init(CURL_GLOBAL_NOTHING); /* minimal */
    _http_handler.set_user_agent("Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36");
    connect_async();
  }

  elasticsearch_producer::~elasticsearch_producer(){

    _http_handler.close();
    close();

    _work.reset();
    _fg.join();
    _bg.join();
  }

  void elasticsearch_producer::close(){
    _closed=true;
  }

  void elasticsearch_producer::connect_async(){
    _connected = true; // TODO login and get an auth token
  }

  void elasticsearch_producer::check_table_exists_async(){
    _table_exists = true;
    _table_create_pending = false;
  }

  static void run_work(std::deque<kspp::async::work<elasticsearch_producer::work_result_t>::async_function>& work, size_t batch_size) {
    size_t parse_errors = 0;
    //size_t timeouts = 0;

    const size_t max_parse_error_per_batch = std::max<size_t>(5, work.size() / 10); // if we get more that 10% per batch there is something seriously wrong (just guessing figures)
    //const size_t max_timeouts_per_batch = std::max<size_t>(5, work.size() / 3);    // if we get more that 30% per batch there is something seriously wrong (just guessing figures)

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
          case elasticsearch_producer::PARSE_ERROR:
            if (++parse_errors < max_parse_error_per_batch)
              work.push_front(batch.get_function(i));
            break;
        }
      }
    }

    if (parse_errors > max_parse_error_per_batch)
      LOG(WARNING) << "parse error threshold exceeded, skipped " << parse_errors - max_parse_error_per_batch << " items";
    //if (timeouts > max_timeouts_per_batch)
    //  LOG(WARNING) << "timeouts threshold exceeded, skipped " << timeouts - max_timeouts_per_batch << " items";
  }

  kspp::async::work<elasticsearch_producer::work_result_t>::async_function  elasticsearch_producer::create_one_http_work(const kspp::generic_avro& key, const kspp::generic_avro* value) {
    auto key_string = avro_simple_column_value(*key.generic_datum());
    std::string url = _cp.url + "/" + _index_name + "/" + "_doc" + "/" + key_string;

    kspp::http::method_t request_type = (value) ? kspp::http::PUT : kspp::http::DELETE_;

    std::string body;

    if (value) {
      //auto key_string = avro2elastic_key_values(*value->valid_schema(), _id_column, *value->generic_datum());
      //key_string.erase(std::remove_if(key_string.begin(), key_string.end(), avro2elastic_IsChars("\"")), key_string.end()); // TODO there should be a key extractor that does not add '' around strings...
      body = avro2elastic_json(*value->valid_schema(), *value->generic_datum());
      //std::string url = _cp.url + "/" + _index_name + "/" + "_doc" + "/" + key_string;
    }

    //std::cerr << body << std::endl;

    kspp::async::work<work_result_t>::async_function f = [this, request_type, body, url](std::function<void(work_result_t)> cb) {
      std::vector<std::string> headers({ "Content-Type: application/json" });
      auto request = std::make_shared<kspp::http::request>(request_type, url, headers, _http_timeout);

      if (_cp.user.size() && _cp.password.size())
        request->set_basic_auth(_cp.user, _cp.password);

      request->append(body);
      request->set_verbose(false);
      _http_handler.perform_async(
        request,
        [this, cb](std::shared_ptr<kspp::http::request> h) {
          //++_http_requests;
          if (!h->ok()) {
            if (!h->transport_result()) {
              //++_http_timeouts;
              DLOG(INFO) << "http transport failed - retrying";
              cb(TIMEOUT);
              return;
            } else {
              // if we are deleteing and the document does not exist we do not consideer this as a failure
              if (h->method()==kspp::http::DELETE_ && h->http_result()==kspp::http::not_found){
                cb(SUCCESS);
                return;
              }
              //++_http_error;
              LOG(WARNING) << "http " << kspp::http::to_string(h->method()) << ", "  << h->uri() << " HTTPRES = " << h->http_result() << " - retrying, reponse:" << h->rx_content();
              cb(HTTP_ERROR);
              return;
            }
          }

          LOG_EVERY_N(INFO, 1000) << "http PUT: " << h->uri() << " got " << h->rx_content_length() << " bytes, time="
                                  << h->milliseconds() << " ms (" << h->rx_kb_per_sec() << " KB/s), #"
                                  << google::COUNTER;
          _msg_bytes += h->tx_content_length();
          cb(SUCCESS);
          // TBD store metrics on request time
        }); // perform_async
    }; // work
    return f;
  }

  void elasticsearch_producer::_process_work() {
    while (!_closed) {
      size_t msg_in_batch = 0 ;
      event_queue<kspp::generic_avro, kspp::generic_avro> in_batch;
      std::deque<kspp::async::work<work_result_t>::async_function> work;
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

        while (!in_batch.empty()) {
          _done.push_back(in_batch.pop_and_get());
        }
        _msg_cnt += msg_in_batch;
      } else {
        std::this_thread::sleep_for(2s);
      }
    }
    LOG(INFO) << "worker thread exiting";
  }

  void elasticsearch_producer::poll() {
    while (!_done.empty()) {
      _done.pop_front();
    }
  }
}