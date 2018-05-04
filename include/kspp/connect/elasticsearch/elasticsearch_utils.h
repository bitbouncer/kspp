#include <avro/Generic.hh>
#include <avro/Schema.hh>
#include <kspp/avro/avro_generic.h>
#pragma once

std::string avro2elastic_to_json(const avro::ValidSchema& schema, const avro::GenericDatum &datum);
std::string avro2elastic_key_values(const avro::ValidSchema& schema, const std::string& key, const avro::GenericDatum &datum);

