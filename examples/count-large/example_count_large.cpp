#include <iostream>
#include <string>
#include <chrono>
#include <regex>
#include <kspp/codecs/text_codec.h>
#include <kspp/topology_builder.h>
#include <kspp/processors/transform.h>
#include <kspp/processors/count.h>

#define NR_OF_PARTITIONS 8

int main(int argc, char **argv) {
  auto text_builder = csi::topology_builder<csi::text_codec>("localhost", "C:\\tmp");
  auto sources = text_builder.create_kafka_sources<void, std::string>("test_text", NR_OF_PARTITIONS);

  std::regex rgx("\\s+");
  auto word_streams = csi::transform_stream<void, std::string, std::string, void>::create(sources, [&rgx](const auto e, auto sink) {
    std::sregex_token_iterator iter(e->value->begin(), e->value->end(), rgx, -1);
    std::sregex_token_iterator end;
    for (; iter != end; ++iter)
      csi::produce(sink, *iter);
  });

  //std::regex rgx("\\s+");
  //auto word_stream = std::make_shared<csi::transform_stream<void, std::string, std::string, void>>(source, [&rgx](const auto e, auto sink) {
  //  std::sregex_token_iterator iter(e->value->begin(), e->value->end(), rgx, -1);
  //  std::sregex_token_iterator end;
  //  for (; iter != end; ++iter)
  //    csi::produce(sink, *iter);
  //});

  {
    for (auto i : word_streams)
      i->start(-2);
    auto word_sink = text_builder.create_kafka_sink<std::string, void>("test_words", -1);
    while (!eof(word_streams))
      consume(word_streams, *word_sink);
  }

/*
word_counts->start(-2); // this does not reset the counts in the backing store.....
  while (!word_counts->eof()) {
    auto msg = word_counts->consume();
    //if (msg) {
*/

  auto word_sources = text_builder.create_kafka_sources<std::string, void>("test_words", NR_OF_PARTITIONS);
  auto word_counts  = text_builder.create_count_keys<std::string>(word_sources);


  for (auto i : word_counts) {
    std::cerr << i->name() << std::endl;
    i->start(-2);
    while (!i->eof())
      i->consume();
  }

  /*
  for (auto i : word_counts)
    i->start(-2);

  while (!eof(word_counts))
    consume(word_counts);
  */

  for (auto i : word_counts)
    for (auto j : *i)
      std::cerr << j->key << " : " << *j->value << std::endl;
}
