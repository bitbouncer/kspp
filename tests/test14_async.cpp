#include <thread>
#include <chrono>
#include <iostream>       // std::cout
#include <future>         // std::async, std::future
#include <boost/asio.hpp>
#include <kspp/utils/async.h>
#include <glog/logging.h>

using namespace std::chrono_literals;

void async_sleep(boost::asio::io_service &ios, int64_t ms, std::function<void(int ec)> cb) {
  auto h = std::make_shared<boost::asio::deadline_timer>(ios);
  h->expires_from_now(boost::posix_time::milliseconds(ms));
  LOG(INFO) << "sleeping for " << " (time : " << ms << " ms)";
  h->async_wait([h, ms, cb](const boost::system::error_code &ec) {
    LOG(INFO) << "sleep is over : " << " (time : " << ms << " ms)";
    cb(!ec ? 0 : -1);
  });
}


void test1() {
  std::cout << "start test 1" << std::endl;
  boost::asio::io_service io_service;
  std::unique_ptr<boost::asio::io_service::work> keepalive_work = std::make_unique<boost::asio::io_service::work>(
      io_service);
  std::thread asio_thread([&] { io_service.run(); });
  auto work = std::make_shared<kspp::async::work<int>>(kspp::async::SEQUENTIAL, kspp::async::FIRST_FAIL);
  for (auto delay: {5, 4, 3, 2, 1}) {
    work->push_back([&io_service, delay](kspp::async::work<int>::callback cb) {
      async_sleep(io_service, delay * 1000, cb);
    });
  }

  work->async_call([](int64_t duration, int ec) {
    LOG(INFO) << "SEQUENTIAL work done ec: " << ec << " (time : " << duration << " ms)";
  });

  keepalive_work.reset();
  asio_thread.join();
  LOG(INFO) << "exiting test1";
}

void test2() {
  LOG(INFO) << "start test 2";
  boost::asio::io_service io_service;
  std::unique_ptr<boost::asio::io_service::work> keepalive_work = std::make_unique<boost::asio::io_service::work>(
      io_service);
  std::thread asio_thread([&] { io_service.run(); });

  auto work = std::make_shared<kspp::async::work<int>>(kspp::async::PARALLEL, kspp::async::FIRST_FAIL);
  for (auto delay: {5, 4, 3, 2, 1}) {
    work->push_back([&io_service, delay](kspp::async::work<int>::callback cb) {
      async_sleep(io_service, delay * 1000, cb);
    });
  }


  auto result = work->call();
  LOG(INFO) << "PARALLEL work done ec: " << result << " (time : " << "???" << " ms)";
  //work->async_call([](int64_t duration, int ec) {
  //  LOG(INFO) << "PARALLEL work done ec: " << ec << " (time : " << " ms)";
  //});

  LOG(INFO) << "resetting dummy work";
  keepalive_work.reset();
  asio_thread.join();
  LOG(INFO) << "exiting test2";
}


int main() {
  test1();
  test2();
  //gflags::ShutDownCommandLineFlags();
  return 0;
}

