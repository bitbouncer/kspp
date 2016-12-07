
#include <iostream>
#include <string>
#include <cstdlib>
#include <cstdio>
#include <csignal>
#include <cstring>
#include <chrono>
#include <kspp/kafka_producer.h>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <kspp/encoder.h>

static bool run = true;

inline boost::uuids::uuid to_uuid(int64_t x) {
  boost::uuids::uuid uuid;
  memset(uuid.data, 0, 16);
  memcpy(uuid.data, &x, 8);
  return uuid;
}

static void sigterm(int sig) {
  run = false;
}

int main(int argc, char **argv) {
  std::string brokers = "localhost";

  signal(SIGINT, sigterm);
  signal(SIGTERM, sigterm);

  // boost::uuids::uuid, boost::uuids::uuid, binary_codec

  auto codec = std::make_shared<csi::binary_codec>();

  auto partitioner = [](const boost::uuids::uuid& key, const int64_t& value)->uint32_t { return value % 8; };

  csi::kafka_sink<boost::uuids::uuid, int64_t, csi::binary_codec>  table_stream("localhost", "kspp_test0_table", codec, partitioner);
  csi::kafka_sink<boost::uuids::uuid, int64_t, csi::binary_codec>  event_stream("localhost", "kspp_test0_eventstream", codec, partitioner);

  std::vector<boost::uuids::uuid> ids;

  auto t0 = std::chrono::high_resolution_clock::now();

  for (int i = 0; i != 10000; ++i) {
    ids.push_back(to_uuid(i));
  }

  for (int64_t update_nr = 0; update_nr != 100; ++update_nr) {
    for (auto & i : ids) {
      table_stream.produce(i, update_nr);
    }
    while (table_stream.queue_len())
      table_stream.poll(0);
  }

  for (int64_t event_nr = 0; event_nr != 100; ++event_nr) {
    for (auto & i : ids) {
      event_stream.produce(i, event_nr);
    }
    while (event_stream.queue_len()) {
      event_stream.poll(0);
    }
  }

  table_stream.close();
  event_stream.close();

  /*
  auto t1 = std::chrono::high_resolution_clock::now();
  std::chrono::duration<float> fs = t1 - t0;
  std::chrono::milliseconds  d = std::chrono::duration_cast<std::chrono::milliseconds>(fs);
  std::cout << d.count()/1000 << "s\n";
  */

  RdKafka::wait_destroyed(5000);
  return 0;
}
