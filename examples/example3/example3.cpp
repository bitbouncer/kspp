#include <iostream>
#include <string>
#include <cstdlib>
#include <cstdio>
#include <csignal>
#include <cstring>
#include <chrono>
#include <kspp/ktable.h>
#include <kspp/kstream.h>
#include <kspp/join_processor.h>
#include <kspp/encoder.h>

static bool run = true;

#define PARTITION 0

static void sigterm(int sig) {
  run = false;
}

struct page_view_data
{
  int64_t     time;
  int64_t     user_id;
  std::string url;
};

inline size_t binary_encode(const page_view_data& obj, std::ostream& dst) {
  size_t sz = 0;
  sz += csi::binary_encode(obj.time, dst);
  sz += csi::binary_encode(obj.user_id, dst);
  sz += csi::binary_encode(obj.url, dst);
  return dst.good() ? sz : 0;
}

inline size_t binary_decode(std::istream& src, page_view_data& obj) {
  size_t sz = 0;
  sz += csi::binary_decode(src, obj.time);
  sz += csi::binary_decode(src, obj.user_id);
  sz += csi::binary_decode(src, obj.url);
  return src.good() ? sz + sizeof(uint32_t) : 0;
}

struct user_profile_data
{
  int64_t     last_modified_time;
  int64_t     user_id;
  std::string email;
};

inline size_t binary_encode(const user_profile_data& obj, std::ostream& dst) {
  size_t sz = 0;
  sz += csi::binary_encode(obj.last_modified_time, dst);
  sz += csi::binary_encode(obj.user_id, dst);
  sz += csi::binary_encode(obj.email, dst);
  return dst.good() ? sz : 0;
}

inline size_t binary_decode(std::istream& src, user_profile_data& obj) {
  size_t sz = 0;
  sz += csi::binary_decode(src, obj.last_modified_time);
  sz += csi::binary_decode(src, obj.user_id);
  sz += csi::binary_decode(src, obj.email);
  return src.good() ? sz + sizeof(uint32_t) : 0;
}

inline size_t binary_encode(const std::pair<int64_t, int64_t>& obj, std::ostream& dst) {
  size_t sz = 0;
  sz += csi::binary_encode(obj.first, dst);
  sz += csi::binary_encode(obj.second, dst);
  return dst.good() ? sz : 0;
}

inline size_t binary_decode(std::istream& src, std::pair<int64_t, int64_t>& obj) {
  size_t sz = 0;
  sz += csi::binary_decode(src, obj.first);
  sz += csi::binary_decode(src, obj.second);
  return src.good() ? sz + sizeof(uint32_t) : 0;
}

int main(int argc, char **argv) {
  std::string brokers = "localhost";

  signal(SIGINT, sigterm);
  signal(SIGTERM, sigterm);

  auto codec = std::make_shared<csi::binary_codec>();

  //auto partitioner = [](const boost::uuids::uuid& key, const int64_t& value)->uint32_t { return value % 8; };

  /*
  kspp_PageViews
  1 = > {"time":1440557383335, "user_id" : 1, "url" : "/home?user=1"}
  5 = > {"time":1440557383345, "user_id" : 5, "url" : "/home?user=5"}
  2 = > {"time":1440557383456, "user_id" : 2, "url" : "/profile?user=2"}
  1 = > {"time":1440557385365, "user_id" : 1, "url" : "/profile?user=1"}
  */

  /*
  kspp_UserProfile
  1 => {"last_modified_time":1440557383335, "user_id":1, "email":"user1@aol.com"}
  5 => {"last_modified_time":1440557383345, "user_id":5, "email":"user5@gmail.com"}
  2 => {"last_modified_time":1440557383456, "user_id":2, "email":"user2@yahoo.com"}
  1 => {"last_modified_time":1440557385365, "user_id":1, "email":"user1-new-email-addr@comcast.com"}
  2 => {"last_modified_time":1440557385395, "user_id":2, "email":null}  <-- user has been deleted
  */

  /*
  kspp_ViewCountsByUser
  1 => {"last_modified_time":1440557383335, "user_id":1, "count":1"}
  5 => {"last_modified_time":1440557383345, "user_id":5, "email":"user5@gmail.com"}
  2 => {"last_modified_time":1440557383456, "user_id":2, "email":"user2@yahoo.com"}
  1 => {"last_modified_time":1440557385365, "user_id":1, "email":"user1-new-email-addr@comcast.com"}
  2 => {"last_modified_time":1440557385395, "user_id":2, "email":null}  <-- user has been deleted
  */

  //csi::kstream_builder builder = new csi::kstream_builder("localhost");
  csi::kstream2<int64_t, page_view_data, csi::binary_codec>    pageviews("localhost", "kspp_PageViews", PARTITION, "C:\\tmp\\example3", codec);
  csi::ktable2<int64_t, user_profile_data, csi::binary_codec>  userprofiles("localhost", "kspp_UserProfile", PARTITION, "C:\\tmp\\example3", codec);
  //csi::ktable2<std::pair<int64_t, int64_t>, int64_t, csi::binary_codec>  viewcountsbyuser("localhost", "kspp_ViewCountsByUser", PARTITION, "C:\\tmp\\example3", codec);
  //csi::kstream2<std::string, std::string, csi::binary_codec>           pagebyuser("localhost", "kspp_ViewCountsByUser", PARTITION, "C:\\tmp\\example3", codec);
  //csi::ktable2<std::string, int64_t, csi::binary_codec>                  pagecounts("localhost", "kspp_PageCounts", PARTITION, "C:\\tmp\\example3", codec);
  
  //csi::ktable2<std::string, int64_t, csi::binary_codec>  pagecounts = pageviews.aggregateByValue()

  //pageviews.add_handler();




  std::cerr << "initing userprofiles store" << std::endl;
  while (run) {
    userprofiles.consume();
    if (userprofiles.eof())
      break;
  }
  userprofiles.flush_offset();
  std::cerr << "done - initing userprofiles store" << std::endl;

  auto t0 = std::chrono::high_resolution_clock::now();

  int64_t join_count = 0;
  int64_t found_count = 0;
  int flush_count = 0;

  while (run) {
    auto ev = pageviews.consume();
    if (ev) {
      join_count++;
      auto row = userprofiles.find(ev->key_pointer(), ev->key_len());
      if (row) {
        found_count++;
        auto e = pageviews.parse(ev);
        auto r = userprofiles.parse(row);
        //e->value->
      }
    }
    if (pageviews.eof())
      break;
  }

  pageviews.close();
  userprofiles.close();

  //RdKafka::wait_destroyed(5000);
  return 0;
}
