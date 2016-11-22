#include "ktable.h"
#include "kspp.h"
#include <assert.h>

namespace csi {
ktable::ktable(std::string brokers, std::string topic, int32_t partition, std::string storage_path) :
  _consumer(brokers, topic, partition),
  _local_storage(storage_path + "\\ktable_" + topic + "_" + std::to_string(partition)) {}

ktable::~ktable() {}

std::unique_ptr<RdKafka::Message> ktable::consume() {
  auto msg = _consumer.consume();
  if (msg) {
    _local_storage.put(msg.get());
  }
  return msg;
}
} // namespace