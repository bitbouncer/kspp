#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <boost/uuid/string_generator.hpp>
#include <ostream>
#include <istream>
#include <vector>
#include <typeinfo>
#include <tuple>
#include <avro/Encoder.hh>
#include <avro/Decoder.hh>
#include <avro/Compiler.hh>
#include <avro/Generic.hh>
#include <avro/Specific.hh>
#include <glog/logging.h>
#include <kspp/avro/generic_avro.h>
#include "grpc_avro_schema_resolver.h"

#pragma once

namespace kspp {

  inline bool is_compatible(const avro::ValidSchema &a, const avro::ValidSchema &b) {
    std::stringstream as;
    std::stringstream bs;
    a.toJson(as);
    b.toJson(bs);
    return as.str() == bs.str();
  }

  class grpc_avro_serdes {
    template<typename T>
    struct fake_dependency : public std::false_type {
    };

  public:
    grpc_avro_serdes(std::shared_ptr<grpc_avro_schema_resolver> resolver, bool relaxed_parsing = false)
        : _resolver(resolver), _relaxed_parsing(relaxed_parsing) {
    }

    static std::string name() { return "kspp::grpc_avro_serdes"; }

    /*
    * read confluent avro format
    * confluent framing marker 0x00 (binary)
    * schema id from registry (htonl - encoded)
    * avro encoded payload
    */
    template<class T>
    size_t decode(int schema_id, const char *payload, size_t size, T &dst) {
      static int32_t expected_schema_id = -1;
      if (expected_schema_id < 0) {
        auto validSchema = _resolver->get_schema(schema_id);
        if (validSchema && is_compatible(*validSchema, *dst.valid_schema())) {
          expected_schema_id = schema_id;
        } else {
          return 0;
        }
      }
      return decode(dst.valid_schema(), payload, size, dst);
    }

    template<class T>
    size_t decode(std::shared_ptr<const avro::ValidSchema> schema, const char *payload, size_t size, T &dst) {
      try {
        auto bin_is = avro::memoryInputStream((const uint8_t *) payload, size);
        avro::DecoderPtr bin_decoder = avro::binaryDecoder();
        bin_decoder->init(*bin_is);
        avro::decode(*bin_decoder, dst);
        return bin_is->byteCount();
      }
      catch (const avro::Exception &e) {
        LOG_FIRST_N(ERROR, 100) << "avro deserialization failed: " << e.what();
        LOG_EVERY_N(ERROR, 1000) << "avro deserialization failed: " << e.what();
        return 0;
      }
    }

  private:
    std::shared_ptr<grpc_avro_schema_resolver> _resolver;
    bool _relaxed_parsing = false;
  };

  template<>
  inline size_t grpc_avro_serdes::decode(int schema_id, const char *payload, size_t size, std::string &dst) {
    using namespace std::string_literals;
    static int32_t expected_schema_id = -1;
    static const std::shared_ptr<const ::avro::ValidSchema> _validSchema(
        std::make_shared<const ::avro::ValidSchema>(::avro::compileJsonSchemaFromString("{\"type\":\"string\"}")));
    if (expected_schema_id < 0) {
      auto validSchema = _resolver->get_schema(schema_id);
      if (validSchema && is_compatible(*validSchema, *_validSchema)) {
        expected_schema_id = schema_id;
      } else {
        return 0;
      }
    }
    return decode(_validSchema, payload, size, dst);
  }

  template<>
  inline size_t grpc_avro_serdes::decode(int schema_id, const char *payload, size_t size, kspp::generic_avro &dst) {
    // if there are empty data we conside that as a null (ie default to for kspp::generic_avro) so just return
    if (size == 0)
      return 0;

    // this should net be in the stream - not possible to decode
    if (schema_id <= 0) {
      LOG(ERROR) << "schema id invalid: " << schema_id;
      return 0;
    }

    auto validSchema = _resolver->get_schema(schema_id);

    if (validSchema == nullptr)
      return 0;

    try {
      auto bin_is = avro::memoryInputStream((const uint8_t *) payload, size);
      avro::DecoderPtr bin_decoder = avro::validatingDecoder(*validSchema, avro::binaryDecoder());
      dst.create(validSchema, schema_id);
      bin_decoder->init(*bin_is);
      avro::decode(*bin_decoder, *dst.generic_datum());
      return bin_is->byteCount();
    }
    catch (const avro::Exception &e) {
      LOG(ERROR) << "avro deserialization failed: " << e.what();
      return 0;
    }
  }
}
