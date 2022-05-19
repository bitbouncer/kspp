#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <boost/uuid/string_generator.hpp>
#include <ostream>
#include <istream>
#include <vector>
#include <typeinfo>
#include <tuple>
#include <glog/logging.h>
#include <kspp/schema_registry/schema_registry_client.h>



#pragma once

namespace kspp {
  class proto_serdes {
    template<typename T>
    struct fake_dependency : public std::false_type {
    };

  public:
    proto_serdes(std::shared_ptr<schema_registry_client> registry)
        : _registry(registry){
    }

    static std::string name() { return "kspp::proto"; }

    template<class PROTO>
    int32_t register_schema(std::string subject) {
      if (schema_id_ >= 0)
        return schema_id_;
      auto json = protobuf_register_schema<PROTO>(_registry, subject);
      LOG(INFO) << json.dump();
      schema_id_ =json["id"];
      return schema_id_;
    }

    /*
    * confluent framing marker 0x00 (binary)
    * schema id from registry (htonl - encoded)
    * protobuf encoded payload
    */
    template<class PROTO>
    size_t encode(const PROTO &src, std::ostream &dst) {
      assert(schema_id_>=0);
      //if (schema_id_ < 0)
      //  schema_id_ = protobuf_register_schema<PROTO>(_registry, subject_);
      return encode(schema_id_, src, dst);
    }

    /*
    * confluent avro encoded data
    * write avro format
    * confluent framing marker 0x00 (binary)
    * schema id from registry (htonl - encoded)
    * avro encoded payload
    */
    /*template<class T>
    size_t encode(const std::string &name, const T &src, std::ostream &dst) {
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
    */

    /*
    * confluent avro encoded data
    * write avro format
    * confluent framing marker 0x00 (binary)
    * schema id from registry (htonl - encoded)
    * avro encoded payload
    */
    template<class T>
    size_t encode(int32_t schema_id, const T &src, std::ostream &dst) {
      size_t before = dst.tellp();
      /* write framing */
      char zero = 0x00;
      dst.write(&zero, 1);
      int32_t encoded_schema_id = htonl(schema_id);
      dst.write((const char *) &encoded_schema_id, 4);
      src.SerializeToOstream(&dst);
      size_t after = dst.tellp();
      return after - before;
    }


    /*
    * confluent framing marker 0x00 (binary)
    * schema id from registry (htonl - encoded)
    * protobuf encoded payload
    */
    template<class PROTO>
    size_t decode(const char *payload, size_t size, PROTO &dst) {
      assert(schema_id_>=0);
      //if (schema_id_ < 0)
      //  schema_id_ = protobuf_register_schema<PROTO>(_registry, subject_);
      return decode(schema_id_, payload, size, dst);
    }

    template<class T>
    size_t decode(int32_t expected_schema_id, const char *payload, size_t size, T &dst) {
      if (expected_schema_id < 0 || size < 5 || payload[0])
        return 0;

      //read framing
      int32_t encoded_schema_id = -1;
      memcpy(&encoded_schema_id, &payload[1], 4);
      int32_t schema_id = ntohl(encoded_schema_id);

      if (schema_id != schema_id_)
        return 0;
      dst.ParseFromArray((const uint8_t *) payload + 5, size - 5);
      return size;
    }

  private:
    /*std::tuple<int32_t, std::shared_ptr<const avro::ValidSchema>>
    _put_schema(std::string schema_name, std::string schema_as_string) {
      auto valid_schema = std::make_shared<const avro::ValidSchema>(
          avro::compileJsonSchemaFromString(schema_as_string));
      auto schema_id = _registry->put_schema(schema_name, valid_schema);
      return std::make_tuple(schema_id, valid_schema);
    }
    */

    std::shared_ptr<schema_registry_client> _registry;
    //std::string subject_; // suppoed to be [ topic-key topic-value ]
    int32_t schema_id_=-1;
  };
}
