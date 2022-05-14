#include <iostream>
#include <chrono>
#include <kspp/topology_builder.h>
#include <kspp/sinks/stream_sink.h>
#include <kspp/processors/ktable.h>
#include <kspp/processors/join.h>
#include <kspp/processors/count.h>
#include <kspp/processors/flat_map.h>
#include <kspp/sources/kafka_source.h>
#include <kspp/state_stores/mem_store.h>
#include <kspp/utils/kafka_utils.h>
#include <kspp/utils/env.h>
#include <kspp/avro/avro_text.h>
#include <kspp/utils/kspp_utils.h>

using namespace std::chrono_literals;
using namespace std::string_literals;

int main(int argc, char **argv) {
  FLAGS_logtostderr = 1;
  google::InitGoogleLogging(argv[0]);

  // for this to work you need to have a avro schema registry running
  // configured in you ENV
  std::string consumer_group("kspp-examples");
  auto config = std::make_shared<kspp::cluster_config>(consumer_group);
  config->load_config_from_env();
  config->validate();
  config->log();

  kspp::topology_builder builder(config);
  auto partitions = kspp::kafka::get_number_partitions(config, "postgres_mqtt_device_auth_view");
  auto partition_list = kspp::get_partition_list(partitions);
  auto topology = builder.create_topology();
  auto sources = topology->create_processors<kspp::kafka_source<void, kspp::generic_avro, void, kspp::avro_serdes>>(
      partition_list, "postgres_mqtt_device_auth_view", config->avro_serdes());
  auto parsed = topology->create_processors<kspp::flat_map<void, kspp::generic_avro, int, std::string>>(sources,
                                                                                                        [](const kspp::krecord<void, kspp::generic_avro> &in,
                                                                                                           auto stream) {
                                                                                                          try {
                                                                                                            auto record = in.value()->record();
                                                                                                            auto id = record.get_optional<int32_t>(
                                                                                                                "id");
                                                                                                            auto pid = record.get_optional<std::string>(
                                                                                                                "pid");
                                                                                                            auto hash = record.get_optional<std::string>(
                                                                                                                "api_key_hash");
                                                                                                            auto broker_uri = record.get_optional<std::string>(
                                                                                                                "broker_url");
                                                                                                            if (id) {
                                                                                                              insert(
                                                                                                                  stream,
                                                                                                                  *id,
                                                                                                                  "nisse"s);
                                                                                                            }
                                                                                                          }
                                                                                                          catch (
                                                                                                              std::exception &e) {
                                                                                                            LOG(ERROR)
                                                                                                                << "not my kind of avro"
                                                                                                                << e.what();
                                                                                                          }
                                                                                                        });
  auto stored_parsed = topology->create_processors<kspp::ktable<int, std::string, kspp::mem_store>>(parsed);

  topology->start(kspp::OFFSET_BEGINNING);
  topology->flush();

  return 0;
}
