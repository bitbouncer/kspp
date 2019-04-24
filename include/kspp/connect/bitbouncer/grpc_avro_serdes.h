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

  inline bool is_compatible(const avro::ValidSchema& a, const  avro::ValidSchema& b){
    std::stringstream as;
    std::stringstream bs;
    a.toJson(as);
    b.toJson(bs);
    return as.str()==bs.str();
  }

  class grpc_avro_serdes
  {
    template<typename T> struct fake_dependency : public std::false_type {};

  public:
    grpc_avro_serdes(std::shared_ptr<grpc_avro_schema_resolver> resolver, bool relaxed_parsing=false)
        : _resolver(resolver)
        , _relaxed_parsing(relaxed_parsing){
    }

    static std::string name() { return "kspp::grpc_avro_serdes"; }

    /*template<class T>
    int32_t register_schema(std::string name, const T& dummy){
      return _registry->put_schema(name, dummy.valid_schema());
    }*/

    /*
    * confluent avro encoded data
    * write avro format
    * confluent framing marker 0x00 (binary)
    * schema id from registry (htonl - encoded)
    * avro encoded payload
    */
    /*template<class T>
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
    }*/

    /*
    * confluent avro encoded data
    * write avro format
    * confluent framing marker 0x00 (binary)
    * schema id from registry (htonl - encoded)
    * avro encoded payload
    */
    /* template<class T>
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
     }*/

    /*
    * confluent avro encoded data
    * write avro format
    * confluent framing marker 0x00 (binary)
    * schema id from registry (htonl - encoded)
    * avro encoded payload
    */
    /* template<class T>
     size_t encode(int32_t schema_id, const T& src, std::ostream& dst) {
       *//* write framing *//*
      char zero = 0x00;
      dst.write(&zero, 1);
      int32_t encoded_schema_id = htonl(schema_id);
      dst.write((const char*) &encoded_schema_id, 4);

      auto bin_os = avro::memoryOutputStream();
      avro::EncoderPtr bin_encoder = avro::binaryEncoder();
      bin_encoder->init(*bin_os.get());
      avro::encode(*bin_encoder, src);
      bin_encoder->flush(); *//* push back unused characters to the output stream again, otherwise content_length will be a multiple of 4096 *//*

      //get the data from the internals of avro stream
      auto v = avro::snapshot(*bin_os.get());
      size_t avro_size = v->size();
      if (avro_size) {
        dst.write((const char*) v->data(), avro_size);
        return avro_size + 5;
      }
      return 5; // this is probably wrong - is there a 0 size avro message???
    }*/


    /*
    * read confluent avro format
    * confluent framing marker 0x00 (binary)
    * schema id from registry (htonl - encoded)
    * avro encoded payload
    */
    template<class T>
    size_t decode(int schema_id, const char* payload, size_t size, T& dst) {
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
    size_t decode(std::shared_ptr<const avro::ValidSchema> schema, const char* payload, size_t size, T& dst) {
      try {
        auto bin_is = avro::memoryInputStream((const uint8_t *) payload, size);
        avro::DecoderPtr bin_decoder = avro::binaryDecoder();
        bin_decoder->init(*bin_is);
        avro::decode(*bin_decoder, dst);
        return bin_is->byteCount();
      }
      catch (const avro::Exception &e) {
        LOG(ERROR) << "Avro deserialization failed: " << e.what();
        return 0;
      }
    }

  private:
    /*std::tuple<int32_t, std::shared_ptr<const avro::ValidSchema>> _put_schema(std::string schema_name, std::string schema_as_string) {
      auto valid_schema = std::make_shared<const avro::ValidSchema>(avro::compileJsonSchemaFromString(schema_as_string));
      auto schema_id = _registry->put_schema(schema_name, valid_schema);
      return std::make_tuple(schema_id, valid_schema);
    }*/

    std::shared_ptr<grpc_avro_schema_resolver> _resolver;
    bool _relaxed_parsing=false;
  };

  /* template<> inline  int32_t avro_serdes::register_schema(std::string name, const std::string& dummy){
     int32_t schema_id=0;
     std::shared_ptr<const avro::ValidSchema> not_used;
     std::tie(schema_id, not_used) = _put_schema(name, "{\"type\":\"string\"}");
     return schema_id;
   }

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
   }*/

  /*
   template<> inline size_t grpc_avro_serdes::decode(const char* payload, size_t size, std::string& dst) {
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
   */

  template<> inline size_t grpc_avro_serdes::decode(int schema_id, const char* payload, size_t size, std::string& dst) {
    using namespace std::string_literals;
    static int32_t expected_schema_id = -1;
    static const std::shared_ptr<const ::avro::ValidSchema> _validSchema(std::make_shared<const ::avro::ValidSchema>(::avro::compileJsonSchemaFromString("{\"type\":\"string\"}")));
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


/* template<> inline  int32_t avro_serdes::register_schema(std::string name, const int64_t& dummy){
   int32_t schema_id=0;
   std::shared_ptr<const avro::ValidSchema> not_used;
   std::tie(schema_id, not_used) = _put_schema(name,  "{\"type\":\"long\"}");
   return schema_id;
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
 }*/

  /*
   template<> inline size_t grpc_avro_serdes::decode(const char* payload, size_t size, int64_t& dst) {
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
   */

  /*template<> inline  int32_t avro_serdes::register_schema(std::string name, const int32_t&){
    int32_t schema_id=0;
    std::shared_ptr<const avro::ValidSchema> not_used;
    std::tie(schema_id, not_used) = _put_schema(name,  "{\"type\":\"int\"}");
    return schema_id;
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
  }*/

  /*
  template<> inline size_t grpc_avro_serdes::decode(const char* payload, size_t size, int32_t& dst) {
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
   */

  /* template<> inline  int32_t avro_serdes::register_schema(std::string name, const bool& dummy){
     int32_t schema_id=0;
     std::shared_ptr<const avro::ValidSchema> not_used;
     std::tie(schema_id, not_used) = _put_schema(name,  "{\"type\":\"boolean\"}");
     return schema_id;
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
   }*/

  /*
   template<> inline size_t grpc_avro_serdes::decode(const char* payload, size_t size, bool& dst) {
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
   */

  /* template<> inline  int32_t avro_serdes::register_schema(std::string name, const float& dummy){
     int32_t schema_id=0;
     std::shared_ptr<const avro::ValidSchema> not_used;
     std::tie(schema_id, not_used) = _put_schema(name,  "{\"type\":\"float\"}");
     return schema_id;
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
   }*/

  /*
   template<> inline size_t grpc_avro_serdes::decode(const char* payload, size_t size, float& dst) {
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
   */

  /* template<> inline int32_t avro_serdes::register_schema(std::string name, const double& dummy){
     int32_t schema_id=0;
     std::shared_ptr<const avro::ValidSchema> not_used;
     std::tie(schema_id, not_used) = _put_schema(name,  "{\"type\":\"double\"}");
     return schema_id;
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
   }*/

  /*
   template<> inline size_t grpc_avro_serdes::decode(const char* payload, size_t size, double& dst) {
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
   */

  /* template<> inline  int32_t avro_serdes::register_schema(std::string name, const std::vector<uint8_t>& dummy){
     int32_t schema_id=0;
     std::shared_ptr<const avro::ValidSchema> not_used;
     std::tie(schema_id, not_used) = _put_schema(name,  "{\"type\":\"bytes\"}");
     return schema_id;
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
   }*/

  /*
   template<> inline size_t grpc_avro_serdes::decode(const char* payload, size_t size, std::vector<uint8_t>& dst) {
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
   */

  /*template<> inline  int32_t avro_serdes::register_schema(std::string name, const boost::uuids::uuid& dummy){
    int32_t schema_id=0;
    std::shared_ptr<const avro::ValidSchema> not_used;
    std::tie(schema_id, not_used) = _put_schema(name,  "{\"type\":\"string\"}");
    return schema_id;
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
  }*/

  /*
  template<> inline size_t grpc_avro_serdes::decode(const char* payload, size_t size, boost::uuids::uuid& dst) {
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
  */

  template<> inline size_t grpc_avro_serdes::decode(int schema_id, const char* payload, size_t size, kspp::generic_avro& dst) {
    // this should net be in the stream - not possible to decode
    if (schema_id<=0) {
      LOG(ERROR) << "schema id invalid: " <<  schema_id;
      return 0;
    }

    auto validSchema  = _resolver->get_schema(schema_id);

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

  /* template<> inline  int32_t avro_serdes::register_schema(std::string name, const kspp::generic_avro& dummy) {
     return _registry->put_schema(name, dummy.valid_schema());
   }

   template<> inline size_t avro_serdes::encode(const kspp::generic_avro& src, std::ostream& dst) {
     assert(src.schema_id()>=0);
     return encode(src.schema_id(), *src.generic_datum(), dst);
   }*/
}
