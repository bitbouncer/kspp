#include <iostream>
#include <chrono>
#include <kspp/impl/serdes/avro/avro_serdes.h>
#include <kspp/impl/serdes/avro/avro_text.h>
#include <kspp/beta/raw_kafka_sink.h>
#include <kspp/beta/raw_kafka_producer.h>
#include <kspp/utils.h>

using namespace std::chrono_literals;

static boost::uuids::uuid to_uuid(int64_t x) {
  boost::uuids::uuid uuid;
  memset(uuid.data, 0, 16);
  memcpy(uuid.data, &x, 8);
  return uuid;
}

int main(int argc, char **argv) {
  // maybe we should add http:// here...
  auto schema_registry = std::make_shared<kspp::avro_schema_registry>(kspp::utils::default_schema_registry_uri());
  auto avro_serdes = std::make_shared<kspp::avro_serdes>(schema_registry);
  auto avro_stream = std::make_shared<kspp::raw_kafka_sink<boost::uuids::uuid, int64_t, kspp::avro_serdes>>(
          kspp::utils::default_kafka_broker_uri(),
          "kspp_test14_raw",
          100ms,
          avro_serdes);

  std::vector<boost::uuids::uuid> ids;
  for (int i = 0; i != 10; ++i)
    ids.push_back(to_uuid(i));

  std::cerr << "creating " << avro_stream->simple_name() << std::endl;
  for (int64_t update_nr = 0; update_nr != 10; ++update_nr) {
    for (auto &i : ids) {

      avro_stream->produce(i, update_nr,  kspp::milliseconds_since_epoch(), [](int64_t offset, int32_t ec)
      {
        if (ec)
          LOG(ERROR) << "produce failed ec:" << ec;
        else
          LOG(INFO) << "produce done - data in kafka @offset: " << offset;
      });
    }
  }

  avro_stream->flush();

  //wait for all messages to flush..
  //while(avro_stream->)

  return 0;
}
