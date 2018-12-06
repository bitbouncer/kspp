#include <chrono>
#include <memory>
#include <boost/bind.hpp>
#include <kspp/utils/async.h>
#include <kspp/utils/http_client.h>

  class my_client {
  public:
    my_client(const std::string& url)
        : _work(new boost::asio::io_service::work(_ios))
        , _bg(boost::bind(&boost::asio::io_service::run, &_ios))
        , _http_handler(_ios, 10) {
      curl_global_init(CURL_GLOBAL_NOTHING); /* minimal */
    }

    ~my_client(){
      _http_handler.close();
      _work.reset();
      _bg.join();
    }

    std::string call(std::string url){
    return "";
    }

  private:
    boost::asio::io_service _ios;
    std::unique_ptr<boost::asio::io_service::work> _work;
    
    std::thread _bg;
    kspp::http::client _http_handler;
  };




int main(int argc, char** argv){
  LOG(INFO) << "starting test";
  {
    my_client client("10.10.20.42:8888/models/images/classification/classify_one.json");
    auto s = client.call("../../doorling_inference_data/b1/16784413.jpg");
  }
  LOG(INFO) << "after destructor";
  return 0;
}

