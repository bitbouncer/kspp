#include <iostream>
#include <string>
#include <chrono>
#include <regex>
#include <kspp/binary_encoder.h>
#include <kspp/topology_builder.h>
#include <kspp/group_by.h>

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
    auto sink = builder.create_kafka_sink<void, std::string>("kspp_TextInput", PARTITION);
    csi::produce<void, std::string>(*sink, "hello kafka streams");
  }

  {
    auto source = builder.create_kafka_source<void, std::string>("kspp_TextInput", PARTITION);
    source->start(-2);
    while (!source->eof()) {
      auto msg = source->consume();
      if (msg) {
        std::cerr << *msg->value << std::endl;
      }
    }
  }

  //{
  //  std::regex rgx("\\s+");
  //  auto source = builder.create_kafka_source<void, std::string>("kspp_TextInput", PARTITION);
  //  auto group_by = std::make_shared<csi::group_by_value<void, std::string, std::string>>(source, [&rgx](const std::string& val, csi::ksink<std::string, size_t>* sink) {
  //    std::sregex_token_iterator iter(val.begin(),
  //                                    val.end(),
  //                                    rgx,
  //                                    -1);
  //    std::sregex_token_iterator end;
  //    for (; iter != end; ++iter)
  //      sink->produce(std::make_shared<csi::krecord<std::string, size_t>>(*iter, 1));
  //  });

  //  group_by->start(-2);
  //  while (!group_by->eof()) {
  //    auto msg = group_by->consume();
  //    if (msg) {
  //      std::cerr << *msg->value << std::endl;
  //    }
  //  }
  //}

  {
    std::regex rgx("\\s+");
    auto source = builder.create_kafka_source<void, std::string>("kspp_TextInput", PARTITION);
    auto word_stream = std::make_shared<csi::transform_stream<void, std::string, std::string, void>>(source, [&rgx](std::shared_ptr<csi::krecord<void, std::string>> e, csi::ksink<std::string, void>* sink) {
      std::sregex_token_iterator iter(e->value->begin(), e->value->end(), rgx, -1);
      std::sregex_token_iterator end;
      for (; iter != end; ++iter)
        sink->produce(std::make_shared<csi::krecord<std::string, void>>(*iter));
    });
    auto word_counts = std::make_shared<csi::count_keys<std::string, csi::binary_codec>>(word_stream, "C:\\tmp", codec);


    word_counts->start(-2);
    while (!word_counts->eof()) {
      auto msg = word_counts->consume();
      if (msg) {
        std::cerr << msg->key << ":" << *msg->value <<std::endl;
      }
    }



  }

}
