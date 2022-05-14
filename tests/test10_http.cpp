#include <chrono>
#include <memory>
#include <curl/curl.h>
#include <boost/asio.hpp>
#include <kspp/utils/async.h>
#include <kspp/utils/http_client.h>

class my_client {
public:
  my_client(const std::string &url)
      : work_(new boost::asio::io_service::work(ios_)), bg_([this]() { ios_.run(); }), http_handler_(ios_, 10) {
    curl_global_init(CURL_GLOBAL_NOTHING); /* minimal */
  }

  ~my_client() {
    http_handler_.close();
    work_.reset();
    bg_.join();
  }

  std::string call(std::string url) {
    return "";
  }

private:
  boost::asio::io_service ios_;
  std::unique_ptr<boost::asio::io_service::work> work_;

  std::thread bg_;
  kspp::http::client http_handler_;
};


int main(int argc, char **argv) {
  LOG(INFO) << "starting test";
  {
    my_client client("10.10.20.42:8888/models/images/classification/classify_one.json");
    auto s = client.call("../../doorling_inference_data/b1/16784413.jpg");
  }
  LOG(INFO) << "after destructor";
  return 0;
}

