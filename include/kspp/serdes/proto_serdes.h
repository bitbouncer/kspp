#include <boost/uuid/string_generator.hpp>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <glog/logging.h>
#include <google/protobuf/message.h>
#include <istream>
#include <kspp/schema_registry/schema_registry_client.h>
#include <ostream>
#include <tuple>
#include <typeinfo>
#include <vector>

#pragma once

namespace kspp {
std::vector<uint8_t> create_index_array(const google::protobuf::Message *);

class proto_serdes {
  template <typename T> struct fake_dependency : public std::false_type {};

public:
  proto_serdes(std::shared_ptr<schema_registry_client> registry) :
      registry_(registry) {}

  static std::string name() { return "kspp::proto"; }

  template <class PROTO> void register_schema(std::string subject) {
    assert(schema_id_ == -1); // should be done once
    // we need to calc an array of used types...
    auto json = protobuf_register_schema<PROTO>(registry_, subject);
    PROTO dummy;
    index_array_ = create_index_array(&dummy);
    LOG(INFO) << json.dump();
    schema_id_ = json["id"];

  }

  template <class PROTO> size_t encode(const PROTO &src, std::ostream &dst) {
    assert(schema_id_ >= 0);
    return encode(schema_id_, src, dst);
  }

  template <class PROTO>
  size_t decode(const char *payload, size_t size, PROTO &dst) {
    assert(schema_id_ >= 0);
    return decode(schema_id_, payload, size, dst);
  }

private:
  /*
   * confluent framing marker 0x00 (binary)
   * schema id from registry (htonl - encoded)
   * sigzag encoded index array
   * protobuf encoded payload
   */
  template <class T>
  size_t encode(int32_t schema_id, const T &src, std::ostream &dst) {
    size_t before = dst.tellp();
    /* write framing */
    char zero = 0x00;
    dst.write(&zero, 1);
    int32_t encoded_schema_id = htonl(schema_id);
    dst.write((const char *)&encoded_schema_id, 4);
    dst.write((const char*) index_array_.data(), index_array_.size());
    src.SerializeToOstream(&dst);
    size_t after = dst.tellp();
    return after - before;
  }

  /*
   * confluent framing marker 0x00 (binary)
   * schema id from registry (htonl - encoded)
   * sigzag encoded index array - we skip this for now
   * protobuf encoded payload
   */
  template <class T>
  size_t decode(int32_t expected_schema_id, const char *payload, size_t size,
                T &dst) {
    size_t header_size = 5 + index_array_.size();
    if (expected_schema_id < 0 || size < header_size || payload[0])
      return 0;

    // read framing
    int32_t encoded_schema_id = -1;
    memcpy(&encoded_schema_id, &payload[1], 4);
    int32_t schema_id = ntohl(encoded_schema_id);

    // skip index_array_.size() bytes for now

    if (schema_id != schema_id_)
      return 0;
    dst.ParseFromArray((const uint8_t *)payload + header_size, size - header_size);
    return size;
  }

  std::shared_ptr<schema_registry_client> registry_;
  int32_t schema_id_ = -1;
  std::vector<uint8_t> index_array_;
};
} // namespace kspp
