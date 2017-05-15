#pragma once
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <ostream>
#include <istream>
#include <vector>
#include <typeinfo>
#include <regex>
#include <avro/Compiler.hh>
#include <avro/Specific.hh>
#include <avro/Encoder.hh>
#include <avro/Decoder.hh>
#include "confluent_schema_registry.h"

namespace kspp {

class avro_schema_registry {
  public:
  avro_schema_registry(std::string urls)
    : _work(new boost::asio::io_service::work(_ios))
    , _thread([this] { _ios.run(); }) 
    , _registry(std::make_shared<confluent::registry>(_ios, split_urls(urls)))
  {
  }

  ~avro_schema_registry() {
    _registry.reset();
    _work.reset();
    _thread.join();
  }

  int32_t put_schema(std::string name, std::shared_ptr<avro::ValidSchema> schema) {
    auto future = _registry->put_schema(name, schema);
    future.wait();
    auto rpc_result = future.get();
    if (rpc_result.ec) {
      std::cerr << "schema_registry put failed: ec" << rpc_result.ec << std::endl;
      return -1;
    }

    std::cerr << "schema_registry put OK: id" << rpc_result.schema_id << std::endl;
    return rpc_result.schema_id;
  }

  std::shared_ptr<avro::ValidSchema> get_schema(int32_t schema_id) {
    auto future = _registry->get_schema(schema_id);
    future.wait();
    auto rpc_result = future.get();
    if (rpc_result.ec) {
      std::cerr << "schema_registry get failed: ec" << rpc_result.ec << std::endl;
      return nullptr;
    }
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

  boost::asio::io_service                        _ios;
  std::unique_ptr<boost::asio::io_service::work> _work;
  std::thread                                    _thread;
  std::shared_ptr<confluent::registry>           _registry;
};

class avro_serdes
{
  template<typename T> struct fake_dependency : public std::false_type {};

  public:
  avro_serdes(std::shared_ptr<avro_schema_registry> registry) 
    : _registry(registry) {
  }

  static std::string name() { return "kspp::avro"; }

  /*
  template<class T>
  size_t encode(const T& src, std::ostream& dst) {
    static_assert(fake_dependency<T>::value, "you must use specialization to provide a encode for T");
  }

  template<class T>
  size_t decode(std::istream& src, T& dst) {
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

  template<class T>
  size_t decode(int32_t expected_schema_id, std::istream& src, T& dst) {
    /* read framing */
    char zero = 0xFF;
    src.read(&zero, 1);
    int32_t encoded_schema_id = -1;
    src.read((char*) &encoded_schema_id, 4);
    int32_t schema_id = ntohl(encoded_schema_id);
    if (!src.good()) {
      // got something that is smaller than framing - bail out
      return 0;
    }
    return 0; // not implemented
  }

  private:
  std::shared_ptr<avro_schema_registry> _registry;
};

template<> inline size_t avro_serdes::encode(const int64_t& src, std::ostream& dst) {
   static int32_t schema_id = -1;
  if (schema_id < 0) {
    auto valid_schema = std::make_shared<avro::ValidSchema>(avro::compileJsonSchemaFromString("{\"type\":\"long\"}"));
    int32_t res = _registry->put_schema("int64_t", valid_schema);
    if (res >= 0)
      schema_id = res;
    else
      return 0;
  }
  return encode(schema_id, src, dst);
}

/*
template<> inline size_t avro_serdes::decode(std::istream& src, std::int64_t& dst) {
  src.read((char*) &dst, sizeof(int64_t));
  return src.good() ? sizeof(int64_t) : 0;
}
*/


template<> inline size_t avro_serdes::encode(const boost::uuids::uuid& src, std::ostream& dst) {
  static int32_t schema_id = -1;
  if (schema_id < 0) {
    auto valid_schema = std::make_shared<avro::ValidSchema>(avro::compileJsonSchemaFromString("{\"type\":\"string\"}"));
    int32_t res = _registry->put_schema("uuid", valid_schema);
    if (res >= 0)
      schema_id = res;
    else
      return 0;
  }
  return encode(schema_id, boost::uuids::to_string(src), dst);
}

/*
template<> inline size_t avro_serdes::decode(std::istream& src, boost::uuids::uuid& dst) {
  src.read((char*) dst.data, 16);
  return src.good() ? 16 : 0;
}
*/

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