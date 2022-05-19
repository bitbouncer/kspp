#include <avro/Encoder.hh>
#include <glog/logging.h>
#include <istream>
#include <kspp/avro/generic_avro.h>
#include <memory>
#include <ostream>
#include <sstream>

#pragma once

namespace kspp {
template <>
inline size_t text_serdes::encode(const generic_avro &src, std::ostream &dst) {
  if (src.valid_schema() == nullptr)
    return 0;

  /* JSON encoder */
  avro::EncoderPtr json_encoder = avro::jsonEncoder(*src.valid_schema());

  /* JSON output stream */
  auto json_os = avro::ostreamOutputStream(dst);

  try {
    /* Encode Avro datum to JSON */
    json_encoder->init(*json_os.get());
    avro::encode(*json_encoder, *src.generic_datum());
    json_encoder->flush();

  } catch (const avro::Exception &e) {
    LOG(ERROR) << "Binary to JSON transformation failed: " << e.what();
    return 0;
  }
  return json_os->byteCount();
}
} // namespace kspp
