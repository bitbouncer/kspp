#include <iostream>
#include <string>
#include <chrono>
#include <regex>
#include <kspp/codecs/text_codec.h>
#include <kspp/topology_builder.h>
#include <kspp/processors/transform.h>
#include <kspp/processors/count.h>

#define PARTITION 0

int main(int argc, char **argv) {
  auto text_builder = csi::topology_builder<csi::text_codec>("localhost", "C:\\tmp");
  auto source = text_builder.create_kafka_source<void, std::string>("kspp_bible", PARTITION);

  std::regex rgx("\\s+");
  auto word_stream = std::make_shared<csi::transform_stream<void, std::string, std::string, void>>(source, [&rgx](const auto e, auto sink) {
    std::sregex_token_iterator iter(e->value->begin(), e->value->end(), rgx, -1);
    std::sregex_token_iterator end;
    for (; iter != end; ++iter)
      sink->produce(std::make_shared<csi::krecord<std::string, void>>(*iter));
  });

  auto word_counts = std::make_shared<csi::count_keys<std::string, csi::text_codec>>(word_stream, "C:\\tmp");

  word_counts->start(-2); // this does not reset the counts in the backing store.....
  while (!word_counts->eof()) {
    auto msg = word_counts->consume();
      //if (msg) {
      //  std::cerr << msg->key << ":" << *msg->value << std::endl;
      //}
  }

  for (auto i : *word_counts)
    std::cerr << i->key << " : " << *i->value << std::endl;
}
