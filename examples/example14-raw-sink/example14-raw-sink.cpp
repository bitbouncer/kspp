#include <iostream>
#include <chrono>
#include <kspp/sinks/kafka_sink.h>
#include <kspp/utils/env.h>
#include <kspp/serdes/avro_serdes.h>

using namespace std::chrono_literals;

static boost::uuids::uuid to_uuid(int64_t x) {
  boost::uuids::uuid uuid;
  memset(uuid.data, 0, 16);
  memcpy(uuid.data, &x, 8);
  return uuid;
}

// How to use a sink without a topology
int main(int argc, char **argv) {
  FLAGS_logtostderr = 1;
  google::InitGoogleLogging(argv[0]);

  std::string consumer_group("kspp-examples");
  auto config = std::make_shared<kspp::cluster_config>(consumer_group);
  config->load_config_from_env();
  config->validate(); // optional

  auto avro_stream = std::make_shared<kspp::kafka_sink<boost::uuids::uuid, int64_t, kspp::avro_serdes, kspp::avro_serdes>>(
      config,
      "kspp_test14_raw",
      config->avro_serdes(),
      config->avro_serdes());

  std::vector<boost::uuids::uuid> ids;
  for (int i = 0; i != 10; ++i)
    ids.push_back(to_uuid(i));

  std::cerr << "creating " << avro_stream->log_name() << std::endl;
  for (int64_t update_nr = 0; update_nr != 10; ++update_nr) {
    for (auto &i : ids)
      insert(*avro_stream, i, update_nr);
  }

  avro_stream->flush();

  //wait for all messages to flush..
  //while(avro_stream->)

  return 0;
}
