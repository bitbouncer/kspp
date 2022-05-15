#include <chrono>
#include <memory>
#include <curl/curl.h>
#include <boost/asio.hpp>
#include <kspp/utils/async.h>
#include <kspp/utils/http_client.h>
#include <memory>
#include <iostream>

class my_http_client {
public:
  my_http_client()
      : work_(new boost::asio::io_service::work(ios_))
      , bg_([this]() { ios_.run(); })
      , http_handler_(ios_, 10) {
    //curl_global_init(CURL_GLOBAL_NOTHING); /* minimal */
  }

  ~my_http_client() {
    http_handler_.close();
    work_.reset();
    bg_.join();
  }

  std::string get(std::string url) {
    std::vector<std::string> headers = {""};
    auto request = std::make_shared<kspp::http::request>(
        kspp::http::GET,
        url,
        headers);
    auto  r2 = http_handler_.perform(request);
    std::string s = r2->rx_content();
    return s;
  }

private:
  boost::asio::io_service ios_;
  std::unique_ptr<boost::asio::io_service::work> work_;
  std::thread bg_;
  kspp::http::client http_handler_;
};


int main(int argc, char **argv) {
  curl_global_init(CURL_GLOBAL_NOTHING); /* minimal */
  LOG(INFO) << "starting test";
  {
    my_http_client client;
    auto s = client.get("www.di.se");
    LOG(INFO) << s;
  }
  LOG(INFO) << "after destructor";
  return 0;
}

