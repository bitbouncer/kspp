#include <iostream>
#include <chrono>
#include <kspp/topology_builder.h>
#include <kspp/sinks/kafka_sink.h>
#include <kspp/sinks/stream_sink.h>
#include <kspp/sinks/null_sink.h>
#include <kspp/processors/ktable.h>
#include <kspp/processors/join.h>
#include <kspp/processors/count.h>
#include <kspp/processors/flat_map.h>
#include <kspp/sources/kafka_source.h>
#include <kspp/state_stores/mem_counter_store.h>
#include <kspp/utils/kafka_utils.h>
#include <kspp/avro/avro_serdes.h>
#include <kspp/avro/avro_text.h>
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

  size_t join_count = 0;

  auto config = std::make_shared<kspp::cluster_config>();
  config->load_config_from_env();
  config->set_consumer_buffering_time(10ms);
  config->set_producer_buffering_time(10ms);
  config->validate();// optional
  config->log(); // optional

  /*config->set_brokers("SSL://localhost:9091");
  config->set_ca_cert_path("/csi/openssl_client_keystore/ca-cert");
  config->set_private_key_path("/csi/openssl_client_keystore/client_P51_client.pem",
                               "/csi/openssl_client_keystore/client_P51_client.key",
                               "abcdefgh");
  config->set_schema_registry(kspp::utils::default_schema_registry_uri());
  config->validate(); // optional
   */

  auto schema_registry = std::make_shared<kspp::avro_schema_registry>(config);
  auto avro_serdes = std::make_shared<kspp::avro_serdes>(schema_registry);

  auto builder = kspp::topology_builder("kspp-examples", argv[0], config);
  {
    auto topology = builder.create_topology();
    auto avro_stream = topology->create_sink<kspp::kafka_sink<boost::uuids::uuid, int64_t, kspp::avro_serdes>>(
            "kspp_test10_avro", avro_serdes);

    topology->start(kspp::OFFSET_BEGINNING);

    std::vector<boost::uuids::uuid> ids;
    for (int i = 0; i != 10; ++i)
      ids.push_back(to_uuid(i));

    for (int64_t update_nr = 0; update_nr != 100; ++update_nr) {
      for (auto &i : ids) {
        avro_stream->produce(i, update_nr);
      }
    }
    topology->flush();
  }

  {
    auto partitions = kspp::kafka::get_number_partitions(config, "kspp_test10_avro");
    auto partition_list = kspp::get_partition_list(partitions);
    auto topology = builder.create_topology();
    auto sources = topology->create_processors<kspp::kafka_source<boost::uuids::uuid, int64_t, kspp::avro_serdes>>(
            partition_list, "kspp_test10_avro", avro_serdes);
    topology->init_metrics();
    topology->start(kspp::OFFSET_BEGINNING);
    auto t0 = std::chrono::high_resolution_clock::now();
    topology->flush();
    auto t1 = std::chrono::high_resolution_clock::now();
    auto fs1 = t1 - t0;
    auto d1 = std::chrono::duration_cast<std::chrono::milliseconds>(fs1);
    int64_t sz = 0;
    for (auto &&i : sources)
      sz += i->get_metric("in_count");
    std::cout << "typed avro consumes: " << sz << " t: " << d1.count() << "ms\n" << std::endl;
    std::cout << "typed avro per sec : " << 1000.0 * sz / (double) d1.count() << std::endl;
  }

  {
    auto partitions = kspp::kafka::get_number_partitions(config, "kspp_test10_avro");
    auto partition_list = kspp::get_partition_list(partitions);
    auto topology = builder.create_topology();
    auto sources = topology->create_processors<kspp::kafka_source<kspp::GenericAvro, kspp::GenericAvro, kspp::avro_serdes>>(
            partition_list, "kspp_test10_avro", avro_serdes);
    topology->init_metrics();
    topology->start(kspp::OFFSET_BEGINNING);
    auto t0 = std::chrono::high_resolution_clock::now();
    topology->flush();
    auto t1 = std::chrono::high_resolution_clock::now();
    auto fs1 = t1 - t0;
    auto d1 = std::chrono::duration_cast<std::chrono::milliseconds>(fs1);
    int64_t sz = 0;
    for (auto &&i : sources)
      sz += i->get_metric("in_count");
    std::cout << "generic avro consumes: " << sz << " t: " << d1.count() << "ms\n" << std::endl;
    std::cout << "generic avro per sec : " << 1000.0 * sz / (double) d1.count() << std::endl;
  }

  {
    auto partitions = kspp::kafka::get_number_partitions(config, "kspp_test10_avro");
    auto partition_list = kspp::get_partition_list(partitions);
    auto topology = builder.create_topology();
    auto sources = topology->create_processors<kspp::kafka_source<kspp::GenericAvro, kspp::GenericAvro, kspp::avro_serdes>>(
            partition_list, "kspp_test10_avro", avro_serdes);
    auto sink = topology->create_sink<kspp::kafka_sink<kspp::GenericAvro, kspp::GenericAvro, kspp::avro_serdes>>(
            sources, "kspp_test10_avro_B", avro_serdes);
    topology->init_metrics();
    topology->start(kspp::OFFSET_BEGINNING);
    auto t0 = std::chrono::high_resolution_clock::now();
    topology->flush();
    auto t1 = std::chrono::high_resolution_clock::now();
    auto fs1 = t1 - t0;
    auto d1 = std::chrono::duration_cast<std::chrono::milliseconds>(fs1);
    int64_t sz = 0;
    for (auto &&i : sources)
      sz += i->get_metric("in_count");

    std::cout << "generic avro read/writes : " << sz << " t: " << d1.count() << "ms\n" << std::endl;
    std::cout << "generic avro read/writes per sec : " << 1000.0 * sz / (double) d1.count() << std::endl;
  }

  // verify that we can read again...
  {
    auto partitions = kspp::kafka::get_number_partitions(config, "kspp_test10_avro_B");
    auto partition_list = kspp::get_partition_list(partitions);
    auto topology = builder.create_topology();
    auto sources = topology->create_processors<kspp::kafka_source<kspp::GenericAvro, kspp::GenericAvro, kspp::avro_serdes>>(
            partition_list, "kspp_test10_avro_B", avro_serdes);
    auto sink = topology->create_sink<kspp::null_sink<kspp::GenericAvro, kspp::GenericAvro>>(sources);
    topology->init_metrics();
    topology->start(kspp::OFFSET_BEGINNING);
    auto t0 = std::chrono::high_resolution_clock::now();
    topology->flush();
    auto t1 = std::chrono::high_resolution_clock::now();
    auto fs1 = t1 - t0;
    auto d1 = std::chrono::duration_cast<std::chrono::milliseconds>(fs1);
    int64_t sz = 0;
    for (auto &&i : sources)
      sz += i->get_metric("in_count");
    std::cout << "generic avro read : " << sz << " t: " << d1.count() << "ms\n" << std::endl;
    std::cout << "generic avro read per sec : " << 1000.0 * sz / (double) d1.count() << std::endl;
  }

  return 0;
}
