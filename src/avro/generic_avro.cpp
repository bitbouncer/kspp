#include <kspp/avro/generic_avro.h>
#include <avro/Specific.hh>
#include <glog/logging.h>


namespace kspp {
  std::string generic_avro::generic_record::to_json() const {
    return "not implemented";
  }
}

std::string to_json(const kspp::generic_avro& src) {
  /* JSON encoder */
  avro::EncoderPtr json_encoder = avro::jsonEncoder(*src.valid_schema());
  std::stringstream ss;

  /* JSON output stream */
  auto json_os = avro::ostreamOutputStream(ss);

  try {
/* Encode Avro datum to JSON */
    json_encoder->init(*json_os.get());
    avro::encode(*json_encoder, *src.generic_datum());
    json_encoder->flush();

  }
  catch (const avro::Exception &e) {
    LOG(ERROR) << "Binary to JSON transformation failed: " << e.what();
    return 0;
  }
  return ss.str();
}
