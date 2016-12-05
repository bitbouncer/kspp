#include "kstream.h"
#include <assert.h>

namespace csi {
kstream::kstream(std::string brokers, std::string topic, int32_t partition, std::string storage_path) :
  _consumer(brokers, topic, partition),
  _local_storage(storage_path + "\\kstream_" + topic + "_" + std::to_string(partition)) {}

kstream::~kstream() {
  close();
}

void kstream::close() {
  _consumer.close();
  _local_storage.close();
}

std::unique_ptr<RdKafka::Message> kstream::consume() {
  auto msg = _consumer.consume();
  if (msg) {
    _local_storage.put(msg.get());
  }
  return msg;
}
} // namespace