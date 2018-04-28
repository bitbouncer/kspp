#include <memory>
#include <boost/optional.hpp>
#include <avro/Generic.hh>
#include <kspp/avro/avro_utils.h>
#pragma once

namespace kspp {
  class avro_generic_record
  {
  public:
    avro_generic_record(const avro::GenericRecord &record)
        : record_(record){
    }

    template<class T>
    T get(std::string name){
      if (!record_.hasField(name))
        throw std::invalid_argument("no such member: " + name);

      const avro::GenericDatum &field = record_.field(name);

      if(field.type() == cpp_to_avro_type<T>())
        return field.value<T>();

      if (field.isUnion()) {
        const avro::GenericUnion &generic_union(field.value<avro::GenericUnion>());
        if(generic_union.datum().type() == cpp_to_avro_type<T>()) {
          return generic_union.datum().value<T>();
        } else if (generic_union.datum().type() == avro::AVRO_NULL) {
          throw std::invalid_argument(std::string("wrong type, expected:") + to_string(cpp_to_avro_type<T>())+ ", actual: " + to_string(generic_union.datum().type()));
        } else {
          //bad type - throw...
          throw std::invalid_argument(std::string("wrong type, expected:") + to_string(cpp_to_avro_type<T>())+ ", actual: " + to_string(generic_union.datum().type()));
        }
      }
      throw std::invalid_argument(std::string("wrong type, expected:") + to_string(cpp_to_avro_type<T>()) +  ", actual: " + to_string(field.type()));
    }


    template<class T>
    boost::optional<T> get_optional(std::string name){
      if (!record_.hasField(name))
        throw std::invalid_argument("no such member: " + name);

      const avro::GenericDatum &field = record_.field(name);

      if(field.type() == cpp_to_avro_type<T>())
        return field.value<T>();

      if (field.isUnion()) {
        const avro::GenericUnion &generic_union(field.value<avro::GenericUnion>());
        if(generic_union.datum().type() == cpp_to_avro_type<T>()) {
          return generic_union.datum().value<T>();
        } else if (generic_union.datum().type() == avro::AVRO_NULL) {
          return boost::none;
        } else {
          //bad type - throw...
          throw std::invalid_argument(std::string("wrong type, expected:") + to_string(cpp_to_avro_type<T>())+ ", actual: " + to_string(generic_union.datum().type()));
        }
      }
      throw std::invalid_argument(std::string("wrong type, expected:") + to_string(cpp_to_avro_type<T>()) +  ", actual: " + to_string(field.type()));
    }


    template<class T>
    T get(std::string name, const T& default_value){
      if (!record_.hasField(name))
        return default_value;

      const avro::GenericDatum &field = record_.field(name);

      if(field.type() == cpp_to_avro_type<T>())
        return field.value<T>();

      if (field.isUnion()) {
        const avro::GenericUnion &generic_union(field.value<avro::GenericUnion>());
        if(generic_union.datum().type() == cpp_to_avro_type<T>()) {
          return generic_union.datum().value<T>();
        } else if (generic_union.datum().type() == avro::AVRO_NULL) {
          return default_value; // should we do this? no value -> default  but null -> null????
          // we could have a default value here....
          //return null or T
        } else {
          //bad type - throw...
          throw std::invalid_argument(std::string("wrong type, expected:") + to_string(cpp_to_avro_type<T>())+ ", actual: " + to_string(generic_union.datum().type()));
        }
      }
      throw std::invalid_argument(std::string("wrong type, expected:") + to_string(cpp_to_avro_type<T>()) +  ", actual: " + to_string(field.type()));
    }

    bool is_null(std::string name){
      if (!record_.hasField(name))
        throw std::invalid_argument("no such member: " + name);

      const avro::GenericDatum &field = record_.field(name);

      if(field.type() == avro::AVRO_NULL) // can this ever happen???
        return true;

      if (field.isUnion()) {
        const avro::GenericUnion &generic_union(field.value<avro::GenericUnion>());
        return (generic_union.datum().type() == avro::AVRO_NULL);
      }

      return false;
    }

  private:
    const avro::GenericRecord& record_;
  };


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

    inline std::shared_ptr<const avro::ValidSchema> valid_schema() const {
      return _valid_schema;
    }

    inline int32_t schema_id() const {
      return _schema_id;
    }

    inline avro::Type type() const {
      return _generic_datum->type();
    }

    avro_generic_record record() const {
      if (_generic_datum->type() == avro::AVRO_RECORD) {
        return avro_generic_record(_generic_datum->value<avro::GenericRecord>());
      } else {
        throw std::invalid_argument(std::string("wrong type, expected: ") + to_string(avro::AVRO_RECORD) + " actual: " + to_string(_generic_datum->type()));
       }
    }


  private:
    std::shared_ptr<avro::GenericDatum>      _generic_datum;
    std::shared_ptr<const avro::ValidSchema> _valid_schema;
    int32_t                                  _schema_id;
  };


}