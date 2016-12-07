#include "ktable.h"
#include "kspp.h"
#include <assert.h>

//namespace csi {
//ktable::ktable(std::string brokers, std::string topic, int32_t partition, std::string root_path) :
//  _storage_path(root_path + "\\ktable_" + topic + "_" + std::to_string(partition)),
//  _offset_storage_path(_storage_path),
//  _consumer(brokers, topic, partition),
//  _local_storage(_storage_path),
//  _last_offset(RdKafka::Topic::OFFSET_BEGINNING),
//  _last_flushed_offset(RdKafka::Topic::OFFSET_BEGINNING)
//{
//  _offset_storage_path /= "\\kafka_offset.bin";
//  if (boost::filesystem::exists(_offset_storage_path))
//  {
//    std::ifstream is(_offset_storage_path.generic_string(), std::ios::binary);
//    int64_t tmp;
//    is.read((char*) &tmp, sizeof(int64_t));
//    if (is.good()) {
//      _last_offset = tmp;
//      _last_flushed_offset = tmp;
//    }
//  }
//  _consumer.start(_last_offset);
//}
//
//ktable::~ktable() {
//  close();
//}
//
//void ktable::close() {
//  _consumer.close();
//  _local_storage.close();
//}
//
//std::unique_ptr<RdKafka::Message> ktable::consume() {
//  auto msg = _consumer.consume();
//  if (msg) {
//    _local_storage.put(msg.get());
//    _last_offset = msg->offset();
//
//    if (_last_offset > (_last_flushed_offset + 10000))
//      flush_offset();
//  }
//  return msg;
//}
//
//void ktable::flush_offset() {
//  if (_last_flushed_offset != _last_offset) {
//    std::ofstream os(_offset_storage_path.generic_string(), std::ios::binary);
//    os.write((char*) &_last_offset, sizeof(int64_t));
//    _last_flushed_offset = _last_offset;
//    os.flush();
//  }
//}
//
//} // namespace