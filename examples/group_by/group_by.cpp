#include <iostream>
#include <string>
#include <chrono>
#include <kspp/binary_encoder.h>
#include <kspp/topology_builder.h>

#define PARTITION 0


struct word_count_data
{
  std::string text;
  int64_t     count;
};

inline size_t binary_encode(const word_count_data& obj, std::ostream& dst) {
  size_t sz = 0;
  sz += csi::binary_encode(obj.text, dst);
  sz += csi::binary_encode(obj.count, dst);
  return dst.good() ? sz : 0;
}

inline size_t binary_decode(std::istream& src, word_count_data& obj) {
  size_t sz = 0;
  sz += csi::binary_decode(src, obj.text);
  sz += csi::binary_decode(src, obj.count);
  return src.good() ? sz : 0;
}

int main(int argc, char **argv) {
  auto codec = std::make_shared<csi::binary_codec>();
  auto builder = csi::topology_builder<csi::binary_codec>("localhost", "C:\\tmp", codec);

  {
    auto sink = builder.create_kafka_sink<void, std::string>("kspp_TextInput", 0);
    csi::produce<void, std::string>(*sink, "hello kafka streams");
  }

  {
    auto source = builder.create_kafka_source<void, std::string>("kspp_TextInput", 0);
    source->start(-2);
    while (!source->eof()) {
      auto msg = source->consume();
      if (msg) {
        std::cerr << *msg->value << std::endl;
      }
    }
  }
}
