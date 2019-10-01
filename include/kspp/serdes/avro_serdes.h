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
#include <kspp/avro/avro_schema_registry.h>
#include <kspp/avro/avro_utils.h>
#pragma once

namespace kspp {
  class avro_serdes
  {
    template<typename T> struct fake_dependency : public std::false_type {};

  public:
    avro_serdes(std::shared_ptr<avro_schema_registry> registry, bool relaxed_parsing)
            : _registry(registry)
            , _relaxed_parsing(relaxed_parsing){
    }

    static std::string name() { return "kspp::avro"; }

    template<class T>
    int32_t register_schema(std::string name, const T& dummy){
      int32_t schema_id=0;
      std::shared_ptr<const avro::ValidSchema> not_used;
      std::tie(schema_id, not_used) = _put_schema(name,  avro_utils::avro_utils<T>::schema_as_string(dummy));
      return schema_id;
    }

    /*
    * confluent avro encoded data
    * write avro format
    * confluent framing marker 0x00 (binary)
    * schema id from registry (htonl - encoded)
    * avro encoded payload
    */
    template<class T>
    size_t encode(const T& src, std::ostream& dst) {
      static int32_t schema_id = -1;
      if (schema_id < 0) {
        int32_t res = _registry->put_schema(src.avro_schema_name(), src.valid_schema());
        if (res >= 0)
          schema_id = res;
        else
          return 0;
      }
      return encode(schema_id, src, dst);
    }

    /*
    * confluent avro encoded data
    * write avro format
    * confluent framing marker 0x00 (binary)
    * schema id from registry (htonl - encoded)
    * avro encoded payload
    */
    template<class T>
    size_t encode(const std::string& name, const T& src, std::ostream& dst) {
      static int32_t schema_id = -1;
      if (schema_id < 0) {
        int32_t res = _registry->put_schema(name, src.valid_schema());
        if (res >= 0)
          schema_id = res;
        else
          return 0;
      }
      return encode(schema_id, src, dst);
    }

    /*
    * confluent avro encoded data
    * write avro format
    * confluent framing marker 0x00 (binary)
    * schema id from registry (htonl - encoded)
    * avro encoded payload
    */
    template<class T>
    size_t encode(int32_t schema_id, const T& src, std::ostream& dst) {
      /* write framing */
      char zero = 0x00;
      dst.write(&zero, 1);
      int32_t encoded_schema_id = htonl(schema_id);
      dst.write((const char*) &encoded_schema_id, 4);

      auto bin_os = avro::memoryOutputStream();
      avro::EncoderPtr bin_encoder = avro::binaryEncoder();
      bin_encoder->init(*bin_os.get());
      avro::encode(*bin_encoder, src);
      bin_encoder->flush(); /* push back unused characters to the output stream again, otherwise content_length will be a multiple of 4096 */

      //get the data from the internals of avro stream
      auto v = avro::snapshot(*bin_os.get());
      size_t avro_size = v->size();
      if (avro_size) {
        dst.write((const char*) v->data(), avro_size);
        return avro_size + 5;
      }
      return 5; // this is probably wrong - is there a 0 size avro message???
    }


    /*
    * read confluent avro format
    * confluent framing marker 0x00 (binary)
    * schema id from registry (htonl - encoded)
    * avro encoded payload
    */
    template<class T>
    size_t decode(const char* payload, size_t size, T& dst) {
      static int32_t schema_id = -1;
      if (schema_id < 0) {
        int32_t res = _registry->put_schema(dst.avro_schema_name(), dst.valid_schema());
        if (res >= 0)
          schema_id = res;
        else
          return 0;
      }
      return decode(schema_id, dst.valid_schema(), payload, size, dst);
    }

    template<class T>
    size_t decode(int32_t expected_schema_id, std::shared_ptr<const avro::ValidSchema> schema, const char* payload, size_t size, T& dst) {
      if (expected_schema_id < 0 || schema == nullptr || size < 5 || payload[0])
        return 0;

      /* read framing */
      int32_t encoded_schema_id = -1;
      memcpy(&encoded_schema_id, &payload[1], 4);
      int32_t schema_id = ntohl(encoded_schema_id);
      if (expected_schema_id != schema_id) {
        if (!_relaxed_parsing) {
          LOG(ERROR) << "expected schema id " << expected_schema_id << ", actual: " << schema_id;
          return 0;
        }
      }

      try {
        auto bin_is = avro::memoryInputStream((const uint8_t *) payload + 5, size - 5);
        avro::DecoderPtr bin_decoder = avro::binaryDecoder();
        bin_decoder->init(*bin_is);
        avro::decode(*bin_decoder, dst);
        return bin_is->byteCount() + 5;
      }
      catch (const avro::Exception &e) {
        LOG(ERROR) << "Avro deserialization failed: " << e.what();
        return 0;
      }
      return 0; // should never get here
    }

  private:
    std::tuple<int32_t, std::shared_ptr<const avro::ValidSchema>> _put_schema(std::string schema_name, std::string schema_as_string) {
      auto valid_schema = std::make_shared<const avro::ValidSchema>(avro::compileJsonSchemaFromString(schema_as_string));
      auto schema_id = _registry->put_schema(schema_name, valid_schema);
      return std::make_tuple(schema_id, valid_schema);
    }

    std::shared_ptr<avro_schema_registry> _registry;
    bool _relaxed_parsing=false;
  };

  template<> inline size_t avro_serdes::encode(const std::string& src, std::ostream& dst) {
    static int32_t schema_id = -1;
    std::shared_ptr<const avro::ValidSchema> not_used;
    if (schema_id > 0)
      return encode(schema_id, src, dst);
    std::tie(schema_id, not_used) = _put_schema("string", "{\"type\":\"string\"}");
    if (schema_id > 0)
      return encode(schema_id, src, dst);
    else
      return 0;
  }

  template<> inline size_t avro_serdes::decode(const char* payload, size_t size, std::string& dst) {
    static int32_t schema_id = -1;
    static std::shared_ptr<const avro::ValidSchema> valid_schema; // this means we never free memory from used schemas???
    if (schema_id>0)
      return decode(schema_id, valid_schema, payload, size, dst);
    std::tie(schema_id, valid_schema) = _put_schema("string", "{\"type\":\"string\"}");
    if (schema_id > 0)
      return decode(schema_id, valid_schema, payload, size, dst);
    else
      return 0;
  }

  template<> inline size_t avro_serdes::encode(const int64_t& src, std::ostream& dst) {
    static int32_t schema_id = -1;
    std::shared_ptr<const avro::ValidSchema> not_used;
    if (schema_id > 0)
      return encode(schema_id, src, dst);
    std::tie(schema_id, not_used) = _put_schema("long", "{\"type\":\"long\"}");
    if (schema_id > 0)
      return encode(schema_id, src, dst);
    else
      return 0;
  }

  template<> inline size_t avro_serdes::decode(const char* payload, size_t size, int64_t& dst) {
    static int32_t schema_id = -1;
    static std::shared_ptr<const avro::ValidSchema> valid_schema; // this means we never free memory from used schemas???
    if (schema_id>0)
      return decode(schema_id, valid_schema, payload, size, dst);
    std::tie(schema_id, valid_schema) = _put_schema("long", "{\"type\":\"long\"}");
    if (schema_id > 0)
      return decode(schema_id, valid_schema, payload, size, dst);
    else
      return 0;
  }

  template<> inline size_t avro_serdes::encode(const int32_t& src, std::ostream& dst) {
    static int32_t schema_id = -1;
    std::shared_ptr<const avro::ValidSchema> not_used;
    if (schema_id > 0)
      return encode(schema_id, src, dst);
    std::tie(schema_id, not_used) = _put_schema("int", "{\"type\":\"int\"}");
    if (schema_id > 0)
      return encode(schema_id, src, dst);
    else
      return 0;
  }

  template<> inline size_t avro_serdes::decode(const char* payload, size_t size, int32_t& dst) {
    static int32_t schema_id = -1;
    static std::shared_ptr<const avro::ValidSchema> valid_schema; // this means we never free memory from used schemas???
    if (schema_id>0)
      return decode(schema_id, valid_schema, payload, size, dst);
    std::tie(schema_id, valid_schema) = _put_schema("int", "{\"type\":\"int\"}");
    if (schema_id > 0)
      return decode(schema_id, valid_schema, payload, size, dst);
    else
      return 0;
  }

  template<> inline size_t avro_serdes::encode(const bool& src, std::ostream& dst) {
    static int32_t schema_id = -1;
    std::shared_ptr<const avro::ValidSchema> not_used;
    if (schema_id > 0)
      return encode(schema_id, src, dst);
    std::tie(schema_id, not_used) = _put_schema("boolean", "{\"type\":\"boolean\"}");
    if (schema_id > 0)
      return encode(schema_id, src, dst);
    else
      return 0;
  }

  template<> inline size_t avro_serdes::decode(const char* payload, size_t size, bool& dst) {
    static int32_t schema_id = -1;
    static std::shared_ptr<const avro::ValidSchema> valid_schema; // this means we never free memory from used schemas???
    if (schema_id>0)
      return decode(schema_id, valid_schema, payload, size, dst);
    std::tie(schema_id, valid_schema) = _put_schema("boolean", "{\"type\":\"boolean\"}");
    if (schema_id > 0)
      return decode(schema_id, valid_schema, payload, size, dst);
    else
      return 0;
  }

  template<> inline size_t avro_serdes::encode(const float& src, std::ostream& dst) {
    static int32_t schema_id = -1;
    std::shared_ptr<const avro::ValidSchema> not_used;
    if (schema_id > 0)
      return encode(schema_id, src, dst);
    std::tie(schema_id, not_used) = _put_schema("float", "{\"type\":\"float\"}");
    if (schema_id > 0)
      return encode(schema_id, src, dst);
    else
      return 0;
  }

  template<> inline size_t avro_serdes::decode(const char* payload, size_t size, float& dst) {
    static int32_t schema_id = -1;
    static std::shared_ptr<const avro::ValidSchema> valid_schema; // this means we never free memory from used schemas???
    if (schema_id>0)
      return decode(schema_id, valid_schema, payload, size, dst);
    std::tie(schema_id, valid_schema) = _put_schema("float", "{\"type\":\"float\"}");
    if (schema_id > 0)
      return decode(schema_id, valid_schema, payload, size, dst);
    else
      return 0;
  }

  template<> inline size_t avro_serdes::encode(const double& src, std::ostream& dst) {
    static int32_t schema_id = -1;
    std::shared_ptr<const avro::ValidSchema> not_used;
    if (schema_id > 0)
      return encode(schema_id, src, dst);
    std::tie(schema_id, not_used) = _put_schema("double", "{\"type\":\"double\"}");
    if (schema_id > 0)
      return encode(schema_id, src, dst);
    else
      return 0;
  }

  template<> inline size_t avro_serdes::decode(const char* payload, size_t size, double& dst) {
    static int32_t schema_id = -1;
    static std::shared_ptr<const avro::ValidSchema> valid_schema; // this means we never free memory from used schemas???
    if (schema_id>0)
      return decode(schema_id, valid_schema, payload, size, dst);
    std::tie(schema_id, valid_schema) = _put_schema("double", "{\"type\":\"double\"}");
    if (schema_id > 0)
      return decode(schema_id, valid_schema, payload, size, dst);
    else
      return 0;
  }

  template<> inline size_t avro_serdes::encode(const std::vector<uint8_t>& src, std::ostream& dst) {
    static int32_t schema_id = -1;
    std::shared_ptr<const avro::ValidSchema> not_used;
    if (schema_id > 0)
      return encode(schema_id, src, dst);
    std::tie(schema_id, not_used) = _put_schema("bytes", "{\"type\":\"bytes\"}");
    if (schema_id > 0)
      return encode(schema_id, src, dst);
    else
      return 0;
  }

  template<> inline size_t avro_serdes::decode(const char* payload, size_t size, std::vector<uint8_t>& dst) {
    static int32_t schema_id = -1;
    static std::shared_ptr<const avro::ValidSchema> valid_schema; // this means we never free memory from used schemas???
    if (schema_id>0)
      return decode(schema_id, valid_schema, payload, size, dst);
    std::tie(schema_id, valid_schema) = _put_schema("bytes", "{\"type\":\"bytes\"}");
    if (schema_id > 0)
      return decode(schema_id, valid_schema, payload, size, dst);
    else
      return 0;
  }

  template<> inline size_t avro_serdes::encode(const boost::uuids::uuid& src, std::ostream& dst) {
    static int32_t schema_id = -1;
    std::shared_ptr<const avro::ValidSchema> not_used;
    if (schema_id > 0)
      return encode(schema_id, boost::uuids::to_string(src), dst);
    std::tie(schema_id, not_used) = _put_schema("uuid", "{\"type\":\"string\"}");
    if (schema_id > 0)
      return encode(schema_id, boost::uuids::to_string(src), dst);
    else
      return 0;
  }

  template<> inline size_t avro_serdes::decode(const char* payload, size_t size, boost::uuids::uuid& dst) {
    static int32_t schema_id = -1;
    static std::shared_ptr<const avro::ValidSchema> valid_schema; // this means we never free memory from used schemas???
    static boost::uuids::string_generator gen;

    if (schema_id > 0) {
      std::string s;
      size_t sz = decode(schema_id, valid_schema, payload, size, s);
      try {
        dst = gen(s);
        return sz;
      }
      catch (...) {
        //log something
        return 0;
      }
    }

    std::tie(schema_id, valid_schema) = _put_schema("uuid", "{\"type\":\"string\"}");

    if (schema_id > 0) {
      std::string s;
      size_t sz = decode(schema_id, valid_schema, payload, size, s);
      try {
        dst = gen(s);
        return sz;
      }
      catch (...) {
        //log something
        return 0;
      }
    } else {
      return 0;
    }
  }

  template<> inline size_t avro_serdes::decode(const char* payload, size_t size, kspp::generic_avro& dst) {
    if (size < 5 || payload[0])
      return 0;

    /* read framing */
    int32_t encoded_schema_id = -1;
    memcpy(&encoded_schema_id, &payload[1], 4);
    int32_t schema_id = ntohl(encoded_schema_id);

    // this should net be in the stream - not possible to decode
    if (schema_id<0) {
      LOG(ERROR) << "schema id invalid: " <<  schema_id;
      return 0;
    }

    auto validSchema  = _registry->get_schema(schema_id);


    if (validSchema == nullptr)
      return 0;

    try {
      auto bin_is = avro::memoryInputStream((const uint8_t *) payload + 5, size - 5);
      avro::DecoderPtr bin_decoder = avro::validatingDecoder(*validSchema, avro::binaryDecoder());
      dst.create(validSchema, schema_id);
      bin_decoder->init(*bin_is);
      avro::decode(*bin_decoder, *dst.generic_datum());
      return bin_is->byteCount() + 5;
    }
    catch (const avro::Exception &e) {
      LOG(ERROR) << "avro deserialization failed: " << e.what();
      return 0;
    }
    return 0; // should never get here
  }

  template<> inline size_t avro_serdes::encode(const kspp::generic_avro& src, std::ostream& dst) {
    assert(src.schema_id()>=0);
    return encode(src.schema_id(), *src.generic_datum(), dst);
  }
}
