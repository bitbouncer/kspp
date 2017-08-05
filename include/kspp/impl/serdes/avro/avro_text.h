#include <memory>
#include <ostream>
#include <istream>
#include <sstream>
#include <avro/Encoder.hh>
#include <kspp/impl/serdes/text_serdes.h>
#include <kspp/impl/serdes/avro/avro_generic.h>
#include <glog/logging.h>
#pragma once

namespace kspp {
template<> inline size_t text_serdes::encode(const GenericAvro& src, std::ostream& dst) {
  if (src.valid_schema() == nullptr)
    return 0;

  /* JSON encoder */
  avro::EncoderPtr json_encoder = avro::jsonEncoder(*src.valid_schema());

  /* JSON output stream */
  //std::ostringstream oss;
  auto json_os = avro::ostreamOutputStream(dst);

  try {
    /* Encode Avro datum to JSON */
    json_encoder->init(*json_os.get());
    avro::encode(*json_encoder, *src.generic_datum());
    json_encoder->flush();

  }
  catch (const avro::Exception& e) {
    LOG(ERROR) << "Binary to JSON transformation failed: " << e.what();
    //errstr = std::string("Binary to JSON transformation failed: ") + e.what();
    return 0;
  }
  //dst << oss.str();
  return json_os->byteCount();
}

/*
template<> inline size_t text_serdes::decode(std::istream& src, std::shared_ptr<avro::GenericDatum>& dst) {
  static boost::uuids::string_generator gen;
  std::string s;
  std::getline(src, s);
  dst = gen(s);
  return s.size();
}
*/
}
