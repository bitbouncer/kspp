#include <memory>
#include <optional>
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

      inline const avro::GenericDatum& get_generic_datum(const std::string& member) const {
        if (!record_.hasField(member))
          throw std::invalid_argument(name() + "." + member + ": no such member, actual: " + to_json());
        return record_.field(member);
      }

      template<class T>
      T get(const std::string& member) const {
        if (!record_.hasField(member))
          throw std::invalid_argument("no such member: " + member);

        const avro::GenericDatum &datum = record_.field(member);

        if(datum.type() == cpp_to_avro_type<T>())
          return datum.value<T>();

        throw std::invalid_argument(name() + "." + member + ":  wrong type, expected:" + to_string(cpp_to_avro_type<T>()) +  ", actual: " + to_string(datum.type()));
      }

      template<class T>
      std::optional<T> get_optional(const std::string& member) const{
        if (!record_.hasField(member))
          throw std::invalid_argument("no such member: " + member);
        const avro::GenericDatum &datum = record_.field(member);

        if (datum.type() == avro::AVRO_NULL)
            return std::nullopt;

        if(datum.type() == cpp_to_avro_type<T>())
          return datum.value<T>();
        throw std::invalid_argument(name() + "." + member + ": wrong type, expected:" + to_string(cpp_to_avro_type<T>()) +  ", actual: " + to_string(datum.type()));
      }

      std::optional<std::string> get_optional_as_string(const std::string& member) const{
        if (!record_.hasField(member))
          throw std::invalid_argument(name() + "." + member + ": no such member");

        const avro::GenericDatum &datum = record_.field(member);
        switch (datum.type()) {
            case avro::AVRO_NULL:
              return std::nullopt;
            case avro::AVRO_STRING :
              return convert<std::string>(datum);
            case avro::AVRO_INT:
              return std::to_string(convert<int32_t>(datum));
            case avro::AVRO_LONG:
              return std::to_string(convert<int64_t>(datum));
            case avro::AVRO_FLOAT:
              return std::to_string(convert<float>(datum));
            case avro::AVRO_DOUBLE:
              return std::to_string(convert<double>(datum));
            case avro::AVRO_BOOL:
              return std::to_string(convert<bool>(datum));
          }
        throw std::invalid_argument(name() + "." + member + ": , cannot convert to string, actual type: "  + to_string(datum.type()));
      }

      template<class T>
      T get(const std::string& member, const T& default_value) const {
        if (!record_.hasField(member))
          return default_value;

        const avro::GenericDatum &datum = record_.field(member);

        if(datum.type() == cpp_to_avro_type<T>())
          return datum.value<T>();

        if(datum.type() == avro::AVRO_NULL)
          return default_value;

        throw std::invalid_argument(name() + "." + member + ": wrong type, expected:" + to_string(cpp_to_avro_type<T>()) +  ", actual: " + to_string(datum.type()));
      }

      bool is_null(const std::string& member) const {
        if (!record_.hasField(member))
          throw std::invalid_argument(name() + "." + member + ": no such member");
        return record_.field(member).type() == avro::AVRO_NULL;
      }

      std::string name() const {
        return record_.schema()->name().fullname();
      }

      std::string to_json() const;

    private:
      const avro::GenericRecord& record_;
    };

    generic_avro()
        : schema_id_(-1) {
    }

    generic_avro(std::shared_ptr<const avro::ValidSchema> s, int32_t schema_id) {
      create(s, schema_id);
    }

    void create(std::shared_ptr<const avro::ValidSchema> s, int32_t schema_id) {
      valid_schema_ = s;
      generic_datum_ = std::make_shared<avro::GenericDatum>(*valid_schema_);
      schema_id_ = schema_id;
    }

    inline std::shared_ptr<avro::GenericDatum> generic_datum() {
      return generic_datum_;
    }

    inline const std::shared_ptr<avro::GenericDatum> generic_datum() const {
      return generic_datum_;
    }

    inline std::shared_ptr<const avro::ValidSchema> valid_schema() const {
      return valid_schema_;
    }

    inline int32_t schema_id() const {
      return schema_id_;
    }

    inline avro::Type type() const {
      return generic_datum_->type();
    }

    generic_avro::generic_record record() const {
      if (generic_datum_->type() == avro::AVRO_RECORD) {
        return generic_avro::generic_record(generic_datum_->value<avro::GenericRecord>());
      } else {
        throw std::invalid_argument(std::string("wrong type, expected: ") + to_string(avro::AVRO_RECORD) + " actual: " + to_string(generic_datum_->type()));
      }
    }

  private:
    std::shared_ptr<avro::GenericDatum> generic_datum_;
    std::shared_ptr<const avro::ValidSchema> valid_schema_;
    int32_t schema_id_;
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

//TODO
template<>
inline std::string kspp::avro_utils<kspp::generic_avro>::schema_name(const kspp::generic_avro& dummy){
  return normalize(*dummy.valid_schema());
}

template<>
inline std::string kspp::avro_utils<kspp::generic_avro>::schema_as_string(const kspp::generic_avro& dummy){
  return normalize(*dummy.valid_schema());
}

template<>
inline std::shared_ptr<const avro::ValidSchema> kspp::avro_utils<kspp::generic_avro>::valid_schema(const kspp::generic_avro& dummy){
  return dummy.valid_schema();
}


std::string to_json(const kspp::generic_avro& src);

