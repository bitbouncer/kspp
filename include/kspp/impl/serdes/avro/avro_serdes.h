#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <boost/uuid/string_generator.hpp>
#include <ostream>
#include <istream>
#include <vector>
#include <typeinfo>
#include <regex>
#include <tuple>
#include <avro/Encoder.hh>
#include <avro/Decoder.hh>
#include <avro/Compiler.hh>
#include <avro/Generic.hh>
#include <avro/Specific.hh>
#include <kspp/impl/serdes/avro/confluent_schema_registry.h>
#include <kspp/impl/serdes/avro/avro_generic.h>
#include <glog/logging.h>
#pragma once

namespace kspp {

  class avro_schema_registry
  {
  public:
    avro_schema_registry(std::string urls)
            : _work(new boost::asio::io_service::work(_ios))
              , _thread([this] { _ios.run(); })
              , _registry(std::make_shared<confluent::registry>(_ios, split_urls(urls))) {}

    ~avro_schema_registry() {
      _registry.reset();
      _work.reset();
      _thread.join();
    }

    int32_t put_schema(std::string name, std::shared_ptr<const avro::ValidSchema> schema) {
      auto future = _registry->put_schema(name, schema);
      future.wait();
      auto rpc_result = future.get();
      if (rpc_result.ec) {
        LOG(ERROR) << "schema_registry put failed: ec" << rpc_result.ec;
        return -1;
      }

      LOG(INFO) << "schema_registry put OK: id" << rpc_result.schema_id;
      return rpc_result.schema_id;
    }

    std::shared_ptr<const avro::ValidSchema> get_schema(int32_t schema_id) {
      //TBD mutex
      std::map<int32_t, std::shared_ptr<const avro::ValidSchema>>::iterator item = _cache.find(schema_id);
      if (item != _cache.end())
        return item->second;

      auto future = _registry->get_schema(schema_id);
      future.wait();
      auto rpc_result = future.get();
      if (rpc_result.ec) {
        LOG(ERROR) << "schema_registry get failed: ec" << rpc_result.ec;
        return nullptr;
      }

      //TBD mutex
      _cache[schema_id] = rpc_result.schema;
      return rpc_result.schema;
    }

  private:
    static std::vector<std::string> split_urls(std::string s) {
      std::vector<std::string> result;
      std::regex rgx("[,\\s+]");
      std::sregex_token_iterator iter(s.begin(), s.end(), rgx, -1);
      std::sregex_token_iterator end;
      for (; iter != end; ++iter)
        result.push_back(*iter);
      return result;
    }

    boost::asio::io_service                               _ios;
    std::unique_ptr<boost::asio::io_service::work>        _work;
    std::thread                                           _thread;
    std::shared_ptr<confluent::registry>                  _registry;
    std::map<int32_t, std::shared_ptr<const avro::ValidSchema>> _cache;
  };

  class avro_serdes
  {
    template<typename T> struct fake_dependency : public std::false_type {};

  public:
    avro_serdes(std::shared_ptr<avro_schema_registry> registry)
            : _registry(registry) {}

    static std::string name() { return "kspp::avro"; }

    ///**
    //* Format Avro datum as JSON according to schema.
    //*/
    //static int avro2json(std::shared_ptr<avro::ValidSchema> avro_schema, std::shared_ptr<avro::GenericDatum> datum, std::string &str, std::string &errstr) {
    //  /* JSON encoder */
    //  avro::EncoderPtr json_encoder = avro::jsonEncoder(*avro_schema);

    //  /* JSON output stream */
    //  std::ostringstream oss;
    //  std::auto_ptr<avro::OutputStream> json_os = avro::ostreamOutputStream(oss);

    //  try {
    //    /* Encode Avro datum to JSON */
    //    json_encoder->init(*json_os.get());
    //    avro::encode(*json_encoder, *datum);
    //    json_encoder->flush();

    //  }
    //  catch (const avro::Exception &e) {
    //    errstr = std::string("Binary to JSON transformation failed: ") + e.what();
    //    return -1;
    //  }

    //  str = oss.str();
    //  return 0;
    //}

    /*
    template<class T>
    size_t encode(const T& src, std::ostream& dst) {
      static_assert(fake_dependency<T>::value, "you must use specialization to provide a encode for T");
    }

    template<class T>
    size_t decode(std::istream& src, T& dst) {
      static_assert(fake_dependency<T>::value, "you must use specialization to provide a decode for T");
    }

    template<class T>
    size_t decode(const int8_t* payload, size_t size, T& dst) {
    static_assert(fake_dependency<T>::value, "you must use specialization to provide a decode for T");
    }
    */


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
        int32_t res = _registry->put_schema(src.name(), src.valid_schema());
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
    size_t encode(std::string name, const T& src, std::ostream& dst) {
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
        int32_t res = _registry->put_schema(dst.name(), dst.valid_schema());
        if (res >= 0)
          schema_id = res;
        else
          return 0;
      }
      return decode(schema_id, payload, size, dst);
    }

    template<class T>
    size_t decode(int32_t expected_schema_id, std::shared_ptr<avro::ValidSchema> schema, const char* payload, size_t size, T& dst) {
      if (expected_schema_id < 0 || schema == nullptr || size < 5 || payload[0])
        return 0;

      /* read framing */
      int32_t encoded_schema_id = -1;
      memcpy(&encoded_schema_id, &payload[1], 4);
      int32_t schema_id = ntohl(encoded_schema_id);
      if (expected_schema_id != schema_id) {
        // print warning - not message for me
        return 0;
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
    std::tuple<int32_t, std::shared_ptr<avro::ValidSchema>> register_schema(std::string schema_name, std::string schema_as_string) {
      auto valid_schema = std::make_shared<avro::ValidSchema>(avro::compileJsonSchemaFromString(schema_as_string));
      auto schema_id = _registry->put_schema(schema_name, valid_schema);
      return std::make_tuple(schema_id, valid_schema);
    }

    std::shared_ptr<avro_schema_registry> _registry;
  };

  template<> inline size_t avro_serdes::encode(const std::string& src, std::ostream& dst) {
    static int32_t schema_id = -1;
    std::shared_ptr<avro::ValidSchema> not_used;
    if (schema_id > 0)
      return encode(schema_id, src, dst);
    std::tie(schema_id, not_used) = register_schema("string", "{\"type\":\"string\"}");
    if (schema_id > 0)
      return encode(schema_id, src, dst);
    else
      return 0;
  }

  template<> inline size_t avro_serdes::decode(const char* payload, size_t size, std::string& dst) {
    static int32_t schema_id = -1;
    static std::shared_ptr<avro::ValidSchema> valid_schema; // this means we never free memory from used schemas???
    if (schema_id>0)
      return decode(schema_id, valid_schema, payload, size, dst);
    std::tie(schema_id, valid_schema) = register_schema("string", "{\"type\":\"string\"}");
    if (schema_id > 0)
      return decode(schema_id, valid_schema, payload, size, dst);
    else
      return 0;
  }

  template<> inline size_t avro_serdes::encode(const int64_t& src, std::ostream& dst) {
    static int32_t schema_id = -1;
    std::shared_ptr<avro::ValidSchema> not_used;
    if (schema_id > 0)
      return encode(schema_id, src, dst);
    std::tie(schema_id, not_used) = register_schema("long", "{\"type\":\"long\"}");
    if (schema_id > 0)
      return encode(schema_id, src, dst);
    else
      return 0;
  }

  template<> inline size_t avro_serdes::decode(const char* payload, size_t size, int64_t& dst) {
    static int32_t schema_id = -1;
    static std::shared_ptr<avro::ValidSchema> valid_schema; // this means we never free memory from used schemas???
    if (schema_id>0)
      return decode(schema_id, valid_schema, payload, size, dst);
    std::tie(schema_id, valid_schema) = register_schema("long", "{\"type\":\"long\"}");
    if (schema_id > 0)
      return decode(schema_id, valid_schema, payload, size, dst);
    else
      return 0;
  }

  template<> inline size_t avro_serdes::encode(const int32_t& src, std::ostream& dst) {
    static int32_t schema_id = -1;
    std::shared_ptr<avro::ValidSchema> not_used;
    if (schema_id > 0)
      return encode(schema_id, src, dst);
    std::tie(schema_id, not_used) = register_schema("int", "{\"type\":\"int\"}");
    if (schema_id > 0)
      return encode(schema_id, src, dst);
    else
      return 0;
  }

  template<> inline size_t avro_serdes::decode(const char* payload, size_t size, int32_t& dst) {
    static int32_t schema_id = -1;
    static std::shared_ptr<avro::ValidSchema> valid_schema; // this means we never free memory from used schemas???
    if (schema_id>0)
      return decode(schema_id, valid_schema, payload, size, dst);
    std::tie(schema_id, valid_schema) = register_schema("int", "{\"type\":\"int\"}");
    if (schema_id > 0)
      return decode(schema_id, valid_schema, payload, size, dst);
    else
      return 0;
  }

  template<> inline size_t avro_serdes::encode(const bool& src, std::ostream& dst) {
    static int32_t schema_id = -1;
    std::shared_ptr<avro::ValidSchema> not_used;
    if (schema_id > 0)
      return encode(schema_id, src, dst);
    std::tie(schema_id, not_used) = register_schema("boolean", "{\"type\":\"boolean\"}");
    if (schema_id > 0)
      return encode(schema_id, src, dst);
    else
      return 0;
  }

  template<> inline size_t avro_serdes::decode(const char* payload, size_t size, bool& dst) {
    static int32_t schema_id = -1;
    static std::shared_ptr<avro::ValidSchema> valid_schema; // this means we never free memory from used schemas???
    if (schema_id>0)
      return decode(schema_id, valid_schema, payload, size, dst);
    std::tie(schema_id, valid_schema) = register_schema("boolean", "{\"type\":\"boolean\"}");
    if (schema_id > 0)
      return decode(schema_id, valid_schema, payload, size, dst);
    else
      return 0;
  }

  template<> inline size_t avro_serdes::encode(const float& src, std::ostream& dst) {
    static int32_t schema_id = -1;
    std::shared_ptr<avro::ValidSchema> not_used;
    if (schema_id > 0)
      return encode(schema_id, src, dst);
    std::tie(schema_id, not_used) = register_schema("float", "{\"type\":\"float\"}");
    if (schema_id > 0)
      return encode(schema_id, src, dst);
    else
      return 0;
  }

  template<> inline size_t avro_serdes::decode(const char* payload, size_t size, float& dst) {
    static int32_t schema_id = -1;
    static std::shared_ptr<avro::ValidSchema> valid_schema; // this means we never free memory from used schemas???
    if (schema_id>0)
      return decode(schema_id, valid_schema, payload, size, dst);
    std::tie(schema_id, valid_schema) = register_schema("float", "{\"type\":\"float\"}");
    if (schema_id > 0)
      return decode(schema_id, valid_schema, payload, size, dst);
    else
      return 0;
  }

  template<> inline size_t avro_serdes::encode(const double& src, std::ostream& dst) {
    static int32_t schema_id = -1;
    std::shared_ptr<avro::ValidSchema> not_used;
    if (schema_id > 0)
      return encode(schema_id, src, dst);
    std::tie(schema_id, not_used) = register_schema("double", "{\"type\":\"double\"}");
    if (schema_id > 0)
      return encode(schema_id, src, dst);
    else
      return 0;
  }

  template<> inline size_t avro_serdes::decode(const char* payload, size_t size, double& dst) {
    static int32_t schema_id = -1;
    static std::shared_ptr<avro::ValidSchema> valid_schema; // this means we never free memory from used schemas???
    if (schema_id>0)
      return decode(schema_id, valid_schema, payload, size, dst);
    std::tie(schema_id, valid_schema) = register_schema("double", "{\"type\":\"double\"}");
    if (schema_id > 0)
      return decode(schema_id, valid_schema, payload, size, dst);
    else
      return 0;
  }

  template<> inline size_t avro_serdes::encode(const std::vector<uint8_t>& src, std::ostream& dst) {
    static int32_t schema_id = -1;
    std::shared_ptr<avro::ValidSchema> not_used;
    if (schema_id > 0)
      return encode(schema_id, src, dst);
    std::tie(schema_id, not_used) = register_schema("bytes", "{\"type\":\"bytes\"}");
    if (schema_id > 0)
      return encode(schema_id, src, dst);
    else
      return 0;
  }

  template<> inline size_t avro_serdes::decode(const char* payload, size_t size, std::vector<uint8_t>& dst) {
    static int32_t schema_id = -1;
    static std::shared_ptr<avro::ValidSchema> valid_schema; // this means we never free memory from used schemas???
    if (schema_id>0)
      return decode(schema_id, valid_schema, payload, size, dst);
    std::tie(schema_id, valid_schema) = register_schema("bytes", "{\"type\":\"bytes\"}");
    if (schema_id > 0)
      return decode(schema_id, valid_schema, payload, size, dst);
    else
      return 0;
  }

  template<> inline size_t avro_serdes::encode(const boost::uuids::uuid& src, std::ostream& dst) {
    static int32_t schema_id = -1;
    std::shared_ptr<avro::ValidSchema> not_used;
    if (schema_id > 0)
      return encode(schema_id, boost::uuids::to_string(src), dst);
    std::tie(schema_id, not_used) = register_schema("uuid", "{\"type\":\"string\"}");
    if (schema_id > 0)
      return encode(schema_id, boost::uuids::to_string(src), dst);
    else
      return 0;
  }

  template<> inline size_t avro_serdes::decode(const char* payload, size_t size, boost::uuids::uuid& dst) {
    static int32_t schema_id = -1;
    static std::shared_ptr<avro::ValidSchema> valid_schema; // this means we never free memory from used schemas???
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

    std::tie(schema_id, valid_schema) = register_schema("uuid", "{\"type\":\"string\"}");

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

  template<> inline size_t avro_serdes::decode(const char* payload, size_t size, kspp::GenericAvro& dst) {
    if (size < 5 || payload[0])
      return 0;

    /* read framing */
    int32_t encoded_schema_id = -1;
    memcpy(&encoded_schema_id, &payload[1], 4);
    int32_t schema_id = ntohl(encoded_schema_id);
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
      std::cerr << "Avro deserialization failed: " << e.what();
      return 0;
    }
    return 0; // should never get here
  }

  template<> inline size_t avro_serdes::encode(const kspp::GenericAvro& src, std::ostream& dst) {
    return encode(src.schema_id(), *src.generic_datum(), dst);
  }

};


//ssize_t AvroImpl::deserialize(Schema **schemap, avro::GenericDatum **datump,
//                              const void *payload, size_t size,
//                              std::string &errstr) {
//  serdes_schema_t *ss;
//
//  /* Read framing */
//  char c_errstr[256];
//  ssize_t r = serdes_framing_read(sd_, &payload, &size, &ss,
//                                  c_errstr, sizeof(c_errstr));
//  if (r == -1) {
//    errstr = c_errstr;
//    return -1;
//  } else if (r == 0 && !*schemap) {
//    errstr = "Unable to decode payload: No framing and no schema specified";
//    return -1;
//  }
//
//  Schema *schema = *schemap;
//  if (!schema) {
//    schema = Serdes::Schema::get(dynamic_cast<HandleImpl*>(this),
//                                 serdes_schema_id(ss), errstr);
//    if (!schema)
//      return -1;
//  }
//
//  avro::ValidSchema *avro_schema = schema->object();
//
//  /* Binary input stream */
//  std::auto_ptr<avro::InputStream> bin_is =
//    avro::memoryInputStream((const uint8_t *) payload, size);
//
//  /* Binary Avro decoder */
//  avro::DecoderPtr bin_decoder = avro::validatingDecoder(*avro_schema,
//                                                         avro::binaryDecoder());
//
//  avro::GenericDatum *datum = new avro::GenericDatum(*avro_schema);
//
//  try {
//    /* Decode binary to Avro datum */
//    bin_decoder->init(*bin_is);
//    avro::decode(*bin_decoder, *datum);
//
//  }
//  catch (const avro::Exception &e) {
//    errstr = std::string("Avro deserialization failed: ") + e.what();
//    delete datum;
//    return -1;
//  }
//
//  *schemap = schema;
//  *datump = datum;
//  return 0;
//}

