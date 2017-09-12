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
#include <kspp/impl/kafka_utils.h>
#include <kspp/utils/env.h>
#include <kspp/avro/avro_serdes.h>
#include <kspp/avro/avro_text.h>

using namespace std::chrono_literals;

template<class T>
T get_nullable_value(const avro::GenericRecord& record, std::string name, T default_value){
  const avro::GenericDatum& field = record.field(name);
  if (field.isUnion()) {
    const avro::GenericUnion& au(field.value<avro::GenericUnion>());
    const avro::GenericDatum& actual_value = au.datum();
    if (actual_value.type()==avro::AVRO_NULL)
      return default_value;
    else
      return actual_value.value<T>();
  }  else {
    return field.value<T>();
  }
}

int main(int argc, char **argv) {
  FLAGS_logtostderr = 1;
  google::InitGoogleLogging(argv[0]);

  auto config = std::make_shared<kspp::cluster_config>();
  config->load_config_from_env();
  config->validate();// optional
  config->log(); // optional

  auto schema_registry = std::make_shared<kspp::avro_schema_registry>(config);
  auto avro_serdes = std::make_shared<kspp::avro_serdes>(schema_registry);
  auto builder = kspp::topology_builder("kspp-examples", argv[0], config);

  auto partitions = kspp::kafka::get_number_partitions(config, "postgres_mqtt_device_auth_view");
  auto partition_list = kspp::get_partition_list(partitions);
  auto topology = builder.create_topology();
  auto sources = topology->create_processors<kspp::kafka_source<void, kspp::GenericAvro, kspp::avro_serdes>>(
          partition_list, "postgres_mqtt_device_auth_view", avro_serdes);
  auto parsed = topology->create_processors<kspp::flat_map<void, kspp::GenericAvro, int, std::string>>(sources, [](std::shared_ptr<const kspp::krecord<void, kspp::GenericAvro>> in, auto flat_map) {
    std::shared_ptr<avro::GenericDatum> datum = in->value()->generic_datum();
    if (datum->type()==avro::AVRO_RECORD) {
      const avro::GenericRecord& r = datum->value<avro::GenericRecord>();
      try {
        int id = get_nullable_value(r, "id", -1);
        std::string pid = get_nullable_value(r, "pid", "");
        std::string hash = get_nullable_value(r, "api_key_hash", "");
        std::string broker_uri = get_nullable_value(r, "broker_url", "");

        flat_map->push_back(
                std::make_shared<kspp::krecord<int, std::string>>(id, "nisse"));
      }
      catch (...){
        std::cerr << "not my kind of avro" << std::endl;
      }


      /*
       * const avro::GenericDatum& d = r.get("member");

      const avro::GenericDatum& f0 = r.fieldAt(0);
      if (f0.type() == avro::AVRO_DOUBLE) {
          //std::cout << "Real: " << f0.value<double>() << std::endl;
      }

      const avro::GenericDatum& f1 = r.fieldAt(0);
      if (f1.type() == avro::AVRO_DOUBLE) {
        //std::cout << "Real: " << f0.value<double>() << std::endl;
      }
       */
    }
  });
  auto stored_parsed = topology->create_processors<kspp::ktable<int, std::string, kspp::mem_store>>(parsed);

  topology->start(kspp::OFFSET_BEGINNING);
  topology->flush();

  return 0;
}
