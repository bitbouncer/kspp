#include <iostream>
#include <string>
#include <chrono>
#include <cassert>
#include <kspp/impl/sources/kafka_consumer.h>
#include <kspp/impl/sinks/kafka_producer.h>

using namespace std::chrono_literals;

struct record
{
  std::string key;
  std::string value;
};

static std::vector<record> test_data = 
{
  { "k0", "v0" },
  { "k1", "v1" },
  { "k2", "v2" },
  { "k3", "v3" }
};

int main(int argc, char** argv) {
  

  {
    kspp::kafka_consumer consumer("localhost", "kspp_test4", 0, "kspp_test_consumer_group");
    assert(consumer.topic() == "kspp_test4");
    assert(consumer.partition() == 0);
    consumer.start(-2); // start from begin
  }


  {
    kspp::kafka_consumer consumer("localhost", "kspp_test4", 0);
    assert(consumer.topic() == "kspp_test4");
    assert(consumer.partition() == 0);
    consumer.start(-1); // start from end
    std::this_thread::sleep_for(1000ms);
    // consumer some and make sure nothing is returned
    auto message = consumer.consume();
    assert(message == nullptr);
    //assert(consumer.eof());

    // produce some
    kspp::kafka_producer producer("localhost", "kspp_test4");
    for (auto i : test_data) {
      int ec = producer.produce(0, kspp::kafka_producer::COPY, (void*)i.key.data(), i.key.size(), (void*)i.value.data(), i.value.size(), nullptr);
    }

    std::vector<record> res;

    std::chrono::time_point<std::chrono::system_clock> end = std::chrono::system_clock::now() + 5000ms;
    while (std::chrono::system_clock::now() < end) {
      auto p = consumer.consume();
      if (p) {
        record r;
        r.key.assign((const char*) p->key_pointer(), p->key_len());
        r.value.assign((const char*)p->payload(), p->len());
        res.push_back(r);
      }
    }
   
    assert(res.size()==4);
    for (int i = 0; i != 4; ++i) {
      assert(res[i].key == test_data[i].key);
      assert(res[i].value == test_data[i].value);
    }
  }


  return 0;
}
