#include <memory>
#include <avro/Generic.hh>
#pragma once

namespace kspp {
class GenericAvro
{
  public:
  GenericAvro()
    : _schema_id(-1) {
  }

  GenericAvro(std::shared_ptr<const avro::ValidSchema> s, int32_t schema_id) {
    create(s, schema_id);
  }

  void create(std::shared_ptr<const avro::ValidSchema> s, int32_t schema_id) {
    _valid_schema = s;
    _generic_datum = std::make_shared<avro::GenericDatum>(*_valid_schema);
    _schema_id = schema_id;
  }

  inline std::shared_ptr<avro::GenericDatum> generic_datum() {
    return _generic_datum;
  }

  inline const std::shared_ptr<avro::GenericDatum> generic_datum() const {
    return _generic_datum;
  }

  inline const avro::ValidSchema* valid_schema() const {
    return _valid_schema.get();
  }

  inline int32_t schema_id() const {
    return _schema_id;
  }

  private:
  std::shared_ptr<avro::GenericDatum>      _generic_datum;
  std::shared_ptr<const avro::ValidSchema> _valid_schema;
  int32_t                                  _schema_id;
};
};