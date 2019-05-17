#include <kspp/avro/avro_utils.h>
namespace kspp{
  std::string to_string(avro::Type t){
    switch (t)
    {
      case avro::AVRO_STRING: return "AVRO_STRING";
      case avro::AVRO_BYTES: return "AVRO_BYTES";
      case avro::AVRO_INT: return "AVRO_INT";
      case avro::AVRO_LONG: return "AVRO_LONG";
      case avro::AVRO_FLOAT: return "AVRO_FLOAT";
      case avro::AVRO_DOUBLE: return "AVRO_DOUBLE";
      case avro::AVRO_BOOL: return "AVRO_BOOL";
      case avro::AVRO_NULL: return "AVRO_NULL";
      case avro::AVRO_RECORD: return "AVRO_RECORD";
      case avro::AVRO_ENUM: return "AVRO_ENUM";
      case avro::AVRO_ARRAY: return "AVRO_ARRAY";
      case avro::AVRO_UNION: return "AVRO_UNION";
      case avro::AVRO_FIXED: return "AVRO_FIXED";
      case avro::AVRO_NUM_TYPES: return "AVRO_NUM_TYPES";
      default:
        return "AVRO_UNKNOWN";
    };
  }

  template<>
  std::shared_ptr<const avro::ValidSchema> avro_utils<std::string>::valid_schema(const std::string& dummy){
    static const std::shared_ptr<const ::avro::ValidSchema> _validSchema(std::make_shared<const ::avro::ValidSchema>(::avro::compileJsonSchemaFromString("{\"type\":\"string\"}")));
    return _validSchema;
  }

  template<>
  std::shared_ptr<const avro::ValidSchema> avro_utils<int64_t>::valid_schema(const int64_t& dummy){
    static const std::shared_ptr<const ::avro::ValidSchema> _validSchema(std::make_shared<const ::avro::ValidSchema>(::avro::compileJsonSchemaFromString("{\"type\":\"long\"}")));
    return _validSchema;
  }

  template<>
  std::shared_ptr<const avro::ValidSchema> avro_utils<int32_t>::valid_schema(const int32_t& dummy){
    static const std::shared_ptr<const ::avro::ValidSchema> _validSchema(std::make_shared<const ::avro::ValidSchema>(::avro::compileJsonSchemaFromString("{\"type\":\"int\"}")));
    return _validSchema;
  }

  template<>
  std::shared_ptr<const avro::ValidSchema> avro_utils<bool>::valid_schema(const bool& dummy){
    static const std::shared_ptr<const ::avro::ValidSchema> _validSchema(std::make_shared<const ::avro::ValidSchema>(::avro::compileJsonSchemaFromString("{\"type\":\"boolean\"}")));
    return _validSchema;
  }

  template<>
  std::shared_ptr<const avro::ValidSchema> avro_utils<float>::valid_schema(const float& dummy){
    static const std::shared_ptr<const ::avro::ValidSchema> _validSchema(std::make_shared<const ::avro::ValidSchema>(::avro::compileJsonSchemaFromString("{\"type\":\"float\"}")));
    return _validSchema;
  }

  template<>
  std::shared_ptr<const avro::ValidSchema> avro_utils<double>::valid_schema(const double& dummy){
    static const std::shared_ptr<const ::avro::ValidSchema> _validSchema(std::make_shared<const ::avro::ValidSchema>(::avro::compileJsonSchemaFromString("{\"type\":\"double\"}")));
    return _validSchema;
  }

  template<>
  std::shared_ptr<const avro::ValidSchema> avro_utils<std::vector<uint8_t>>::valid_schema(const std::vector<uint8_t>& dummy){
    static const std::shared_ptr<const ::avro::ValidSchema> _validSchema(std::make_shared<const ::avro::ValidSchema>(::avro::compileJsonSchemaFromString("{\"type\":\"bytes\"}")));
    return _validSchema;
  }

  template<>
  std::shared_ptr<const avro::ValidSchema> avro_utils<boost::uuids::uuid>::valid_schema(const boost::uuids::uuid& dummy){
    static const std::shared_ptr<const ::avro::ValidSchema> _validSchema(std::make_shared<const ::avro::ValidSchema>(::avro::compileJsonSchemaFromString("{\"type\":\"string\"}")));
    return _validSchema;
  }
}