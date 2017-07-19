#include <iostream>
#include <chrono>
#include <kspp/impl/serdes/avro/avro_serdes.h>
#include <kspp/impl/serdes/avro/avro_text.h>
#include <kspp/topology_builder.h>
#include <kspp/sinks/stream_sink.h>
#include <kspp/processors/ktable.h>
#include <kspp/processors/join.h>
#include <kspp/processors/count.h>
#include <kspp/processors/flat_map.h>
#include <kspp/processors/kafka_source.h>
#include <kspp/state_stores/mem_store.h>
#include <kspp/impl/kafka_utils.h>

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
  // maybe we should add http:// here...
  auto schema_registry = std::make_shared<kspp::avro_schema_registry>("localhost:8081");
  auto avro_serdes = std::make_shared<kspp::avro_serdes>(schema_registry);
  auto app_info = std::make_shared<kspp::app_info>("kspp-examples", "example12-generic-avro");
  auto builder = kspp::topology_builder(app_info, "localhost", 1000ms);

  auto partitions = kspp::kafka::get_number_partitions(builder.brokers(), "postgres_mqtt_device_auth_view");
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
        std::string hash = get_nullable_value(r, "hash", "");

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

  topology->start(-2);
  topology->flush();

  return 0;
}
