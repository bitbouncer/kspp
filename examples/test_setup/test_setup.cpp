
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

static bool run = true;


inline boost::uuids::uuid to_uuid(int64_t x) {
  boost::uuids::uuid uuid;
  memset(uuid.data, 0, 16);
  memcpy(uuid.data, &x, 8);
  return uuid;
}

/* low order 32 bits? */
inline uint32_t uuid_to_partition_hash(const boost::uuids::uuid& uuid) {
  uint32_t hash;
  memcpy(&hash, uuid.data, sizeof(uint32_t));
  return hash;
}

static void sigterm(int sig) {
  run = false;
}

int main(int argc, char **argv) {
  std::string brokers = "localhost";

  signal(SIGINT, sigterm);
  signal(SIGTERM, sigterm);

  csi::kafka_producer  table_stream("localhost", "kspp_test0_table");
  csi::kafka_producer  event_stream("localhost", "kspp_test0_eventstream");

  std::vector<boost::uuids::uuid> ids;

  auto t0 = std::chrono::high_resolution_clock::now();

  for (int i = 0; i != 1000000; ++i) {
    ids.push_back(to_uuid(i));
  }


  for (int64_t update_nr = 0; update_nr != 100; ++update_nr) {
    for (auto & i : ids) {
      table_stream.produce(uuid_to_partition_hash(i.data) % table_stream.nr_of_partitions(), csi::kafka_producer::COPY, &i.data, 16, &update_nr, sizeof(update_nr));
    }
  }


  for (int64_t event_nr = 0; event_nr != 10000; ++event_nr) {
    for (auto & i : ids) {
      event_stream.produce(uuid_to_partition_hash(i.data) % event_stream.nr_of_partitions(), csi::kafka_producer::COPY, &i.data, 16, &event_nr, sizeof(event_nr));
    }
  }

  /*while (run) {
    kt.consume();
    if (kt.eof())
      break;
  }*/


 
 /* int64_t join_count = 0;
  int64_t found_count = 0;
  while (run) {
    auto msg = ks.consume();
    if (msg) {
      join_count++;
      auto j = kt.find(msg->key_pointer(), msg->key()->size());
      if (j)
        found_count++;
    }
    if (ks.eof())
      break;
  }*/


  table_stream.close();
  event_stream.close();

  auto t1 = std::chrono::high_resolution_clock::now();
  std::chrono::duration<float> fs = t1 - t0;
  std::chrono::milliseconds  d = std::chrono::duration_cast<std::chrono::milliseconds>(fs);
  std::cout << d.count()/1000 << "s\n";

  //std::cout << "lookups per sec : " << join_count / (d.count()/1000) << std::endl;


  /*
  std::cerr << "% Consumed " << msg_cnt << " messages ("
    << msg_bytes << " bytes)" << std::endl;
    */

  std::cerr << "join: " << join_count << " matched " << found_count << std::endl;

  RdKafka::wait_destroyed(5000);
  return 0;
}
