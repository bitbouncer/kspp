#include <iostream>
#include <chrono>
#include <kspp/avro/avro_serdes.h>
#include <kspp/sinks/raw_kafka_sink.h>
#include <kspp/utils/env.h>

using namespace std::chrono_literals;

static boost::uuids::uuid to_uuid(int64_t x) {
  boost::uuids::uuid uuid;
  memset(uuid.data, 0, 16);
  memcpy(uuid.data, &x, 8);
  return uuid;
}

int main(int argc, char **argv) {
  FLAGS_logtostderr = 1;
  google::InitGoogleLogging(argv[0]);

  auto config = std::make_shared<kspp::cluster_config>();
  config->set_brokers("SSL://localhost:9091");
  config->set_ca_cert_path("/csi/openssl_client_keystore/ca-cert");
  config->set_private_key_path("/csi/openssl_client_keystore/client_P51_client.pem",
                               "/csi/openssl_client_keystore/client_P51_client.key",
                               "abcdefgh");
  config->set_schema_registry(kspp::default_schema_registry_uri());
  config->validate(); // optional

  // todo - this is not ssl ready must take a cluster config to get keys
  auto schema_registry = std::make_shared<kspp::avro_schema_registry>(config);
  auto avro_serdes = std::make_shared<kspp::avro_serdes>(schema_registry);
  auto avro_stream = std::make_shared<kspp::raw_kafka_sink<boost::uuids::uuid, int64_t, kspp::avro_serdes>>(
      config,
      "kspp_test14_raw",
      avro_serdes);

  std::vector<boost::uuids::uuid> ids;
  for (int i = 0; i != 10; ++i)
    ids.push_back(to_uuid(i));

  std::cerr << "creating " << avro_stream->simple_name() << std::endl;
  for (int64_t update_nr = 0; update_nr != 10; ++update_nr) {
    for (auto &i : ids) {

      avro_stream->produce(i, update_nr, kspp::milliseconds_since_epoch(), [](int64_t offset, int32_t ec) {
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
