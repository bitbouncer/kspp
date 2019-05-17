#include <memory>
#include <boost/optional.hpp>
#include <avro/Generic.hh>
#include <kspp/avro/avro_utils.h>
#pragma once

namespace kspp {
  class generic_avro
  {
  public:
    class generic_record
    {
    public:
      generic_record(const avro::GenericRecord &record)
          : record_(record){
      }

      inline const avro::GenericDatum& getGenericDatum(std::string name) const {
        if (!record_.hasField(name))
          throw std::invalid_argument(std::string("no such member: ") + name +  ", actual: " + to_json());

        return record_.field(name);
      }

      template<class T>
      T get(std::string name) const {
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
      boost::optional<T> get_optional(const std::string& name) const{
        if (!record_.hasField(name))
          throw std::invalid_argument("no such member: " + name);

        const avro::GenericDatum &field = record_.field(name);

        if (field.isUnion()) {
          const avro::GenericUnion &generic_union(field.value<avro::GenericUnion>());
          if (generic_union.datum().type() == avro::AVRO_NULL)
            return boost::none;
          else
            return convert<T>(generic_union.datum());
        } else {
          return convert<T>(field);
        }
      }

      boost::optional<std::string> get_optional_as_string(const std::string& name) const{
        if (!record_.hasField(name))
          throw std::invalid_argument("no such member: " + name);

        const avro::GenericDatum &field = record_.field(name);

        if (field.isUnion()) {
          const avro::GenericUnion &generic_union(field.value<avro::GenericUnion>());
          switch (generic_union.datum().type()) {
            case avro::AVRO_NULL:
              return boost::none;
            case avro::AVRO_STRING :
              return convert<std::string>(generic_union.datum());
            case avro::AVRO_INT:
              return std::to_string(convert<int32_t>(generic_union.datum()));
            case avro::AVRO_LONG:
              return std::to_string(convert<int64_t>(generic_union.datum()));
            case avro::AVRO_FLOAT:
              return std::to_string(convert<float>(generic_union.datum()));
            case avro::AVRO_DOUBLE:
              return std::to_string(convert<double>(generic_union.datum()));
            case avro::AVRO_BOOL:
              return std::to_string(convert<bool>(generic_union.datum()));
          }
        } else {
          switch (field.type()) {
            case avro::AVRO_NULL:
              return boost::none;
            case avro::AVRO_STRING :
              return convert<std::string>(field);
            case avro::AVRO_INT:
              return std::to_string(convert<int32_t>(field));
            case avro::AVRO_LONG:
              return std::to_string(convert<int64_t>(field));
            case avro::AVRO_FLOAT:
              return std::to_string(convert<float>(field));
            case avro::AVRO_DOUBLE:
              return std::to_string(convert<double>(field));
            case avro::AVRO_BOOL:
              return std::to_string(convert<bool>(field));
          }
        }
        return boost::none; // TODO not a good default - throw exception
      }


      template<class T>
      T get(std::string name, const T& default_value) const {
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

      bool is_null(std::string name) const {
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

      std::string to_json() const;

    private:
      const avro::GenericRecord& record_;
    };

    generic_avro()
        : _schema_id(-1) {
    }

    generic_avro(std::shared_ptr<const avro::ValidSchema> s, int32_t schema_id) {
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

    generic_avro::generic_record record() const {
      if (_generic_datum->type() == avro::AVRO_RECORD) {
        return generic_avro::generic_record(_generic_datum->value<avro::GenericRecord>());
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

template <> struct avro::codec_traits<kspp::generic_avro> {
  static void encode(avro::Encoder& e, const kspp::generic_avro& ga) {
    avro::GenericWriter::write(e, *ga.generic_datum(), *ga.valid_schema());
  }

  static void decode(Decoder& d, kspp::generic_avro& ga) {
    GenericReader::read(d, *ga.generic_datum(), *ga.valid_schema());
  }
};

template<>
inline std::shared_ptr<const avro::ValidSchema> kspp::avro_utils<kspp::generic_avro>::valid_schema(const kspp::generic_avro& dummy){
  return dummy.valid_schema();
}

std::string to_json(const kspp::generic_avro& src);

