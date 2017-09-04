#include <iostream>
#include <chrono>
#include <cassert>
#include <kspp/impl/sources/kafka_consumer.h>
#include <kspp/impl/sinks/kafka_producer.h>

using namespace std::chrono_literals;

struct record {
  std::string key;
  std::string value;
};

static std::vector<record> test_data =
        {
                {"k0", "v0"},
                {"k1", "v1"},
                {"k2", "v2"},
                {"k3", "v3"}
        };

int main(int argc, char **argv) {
  auto config = std::make_shared<kspp::cluster_config>();
  config->set_brokers("localhost");
  config->set_producer_buffering_time(100ms);
  config->set_consumer_buffering_time(100ms);

  kspp::kafka_producer producer(config, "kspp_test4");

  // create a dummy consumer just to trigger kafka broker
  //{
  //  kspp::kafka_consumer consumer("localhost", "kspp_test4", 0, "kspp_test");
  //  consumer.start(-1); // start from end
  //}

  // test 1
  // start from end and produce 4 records - make sure we get same 4 records
  {
    kspp::kafka_consumer consumer(config, "kspp_test4", 0, "kspp_test");
    int64_t timestamp0 = 1234567890;
    assert(consumer.topic() == "kspp_test4");
    assert(consumer.partition() == 0);
    consumer.start(-1); // start from end
    std::this_thread::sleep_for(1000ms);
    // consumer some and make sure nothing is returned
    auto message = consumer.consume();

    assert(message == nullptr);
    //assert(consumer.eof());

    // produce some
    {
      for (auto i : test_data) {
        int ec = producer.produce(0, kspp::kafka_producer::COPY, (void *) i.key.data(), i.key.size(),
                                  (void *) i.value.data(), i.value.size(), timestamp0, nullptr);
        if (ec) {
          std::cerr << ", failed to produce, reason:" << RdKafka::err2str((RdKafka::ErrorCode) ec) << std::endl;
        }
      }
      assert(producer.flush(1000) == 0);
    }

    std::vector<record> res;

    // prepare to wait a long time for the 4 messages if we started on a new kafka broker
    std::chrono::time_point<std::chrono::system_clock> end = std::chrono::system_clock::now() + 60000ms;
    while (std::chrono::system_clock::now() < end) {
      auto p = consumer.consume();
      if (p) {
        int64_t offset = p->offset();
        record r;
        r.key.assign((const char *) p->key_pointer(), p->key_len());
        r.value.assign((const char *) p->payload(), p->len());
        assert(p->timestamp().timestamp == timestamp0);
        res.push_back(r);
      }
      if (res.size() == 4)
        break;
    }

    assert(res.size() == 4);
    for (int i = 0; i != 4; ++i) {
      assert(res[i].key == test_data[i].key);
      assert(res[i].value == test_data[i].value);
    }
  } // 1

  // test 2
  // start from end
  // commit offset
  // and produce 4 records - make sure we get same 4 records
  //
  {
    int64_t last_comitted_offset = -1;
    int64_t timestamp2 = 2234567890;
    { // test 2 phase A
      kspp::kafka_consumer consumer(config, "kspp_test4", 0, "kspp_test");
      assert(consumer.topic() == "kspp_test4");
      assert(consumer.partition() == 0);
      consumer.start(-1); // start from end
      std::this_thread::sleep_for(1000ms);
      // consumer some and make sure nothing is returned
      auto message = consumer.consume();
      assert(message == nullptr);
      //assert(consumer.eof());

      {
        // produce some
        for (auto i : test_data) {
          int ec = producer.produce(0, kspp::kafka_producer::COPY, (void *) i.key.data(), i.key.size(),
                                    (void *) i.value.data(), i.value.size(), timestamp2, nullptr);
        }
        assert(producer.flush(1000) == 0);
      }

      {
        std::vector<record> res;
        std::chrono::time_point<std::chrono::system_clock> end = std::chrono::system_clock::now() + 5000ms;
        while (std::chrono::system_clock::now() < end) {
          auto p = consumer.consume();
          if (p) {
            if (last_comitted_offset == -1)
              last_comitted_offset = p->offset();
            record r;
            r.key.assign((const char *) p->key_pointer(), p->key_len());
            r.value.assign((const char *) p->payload(), p->len());
            assert(p->timestamp().timestamp == timestamp2);
            res.push_back(r);
          }
          if (res.size() == 4)
            break;
        }

        assert(res.size() == 4);
        for (int i = 0; i != 4; ++i) {
          assert(res[i].key == test_data[i].key);
          assert(res[i].value == test_data[i].value);
        }
      }
      std::cout << "comitting " << last_comitted_offset << std::endl;
      assert(consumer.commit(last_comitted_offset, true) == 0);
      consumer.stop();
    } // 2 phase A

    { // 2 phase B we shouyld pick up 3 records since we comitted one
      kspp::kafka_consumer consumer(config, "kspp_test4", 0, "kspp_test");
      assert(consumer.topic() == "kspp_test4");
      assert(consumer.partition() == 0);
      consumer.start(); // start from after last committed offset

      int64_t first_offset = -1;
      {
        std::vector<record> res;
        std::chrono::time_point<std::chrono::system_clock> end = std::chrono::system_clock::now() + 5000ms;
        while (std::chrono::system_clock::now() < end) {
          auto p = consumer.consume();
          if (p) {
            if (first_offset == -1)
              first_offset = p->offset();
            record r;
            r.key.assign((const char *) p->key_pointer(), p->key_len());
            r.value.assign((const char *) p->payload(), p->len());
            assert(p->timestamp().timestamp == timestamp2);
            res.push_back(r);
          }
          if (res.size() == 3)
            break;
        }
        std::cout << "reading first offset " << first_offset << std::endl;
        assert(first_offset == last_comitted_offset + 1);
        assert(res.size() == 3);
        for (int i = 0; i != 3; ++i) {
          assert(res[i].key == test_data[i + 1].key);
          assert(res[i].value == test_data[i + 1].value);
        }
      }
    } //// test 2 phase B
  }// test 2

  // test 3
  // start from end
  // commit offset
  // and produce 4 records - make sure we get same 4 records
  // stop
  // restart same consumer from committed offset
  {
    int64_t timestamp3 = 3234567890;
    int64_t last_comitted_offset = -1;
    kspp::kafka_consumer consumer(config, "kspp_test4", 0, "kspp_test");
    assert(consumer.topic() == "kspp_test4");
    assert(consumer.partition() == 0);
    consumer.start(-1); // start from end
    std::this_thread::sleep_for(1000ms);
    // consumer some and make sure nothing is returned
    auto message = consumer.consume();
    assert(message == nullptr);
    //assert(consumer.eof());

    // produce some
    {
      for (auto i : test_data) {
        int ec = producer.produce(0, kspp::kafka_producer::COPY, (void *) i.key.data(), i.key.size(),
                                  (void *) i.value.data(), i.value.size(), timestamp3, nullptr);
      }
      assert(producer.flush(1000) == 0);
    }

    {
      std::vector<record> res;
      std::chrono::time_point<std::chrono::system_clock> end = std::chrono::system_clock::now() + 5000ms;
      while (std::chrono::system_clock::now() < end) {
        auto p = consumer.consume();
        if (p) {
          if (last_comitted_offset == -1)
            last_comitted_offset = p->offset();
          record r;
          r.key.assign((const char *) p->key_pointer(), p->key_len());
          r.value.assign((const char *) p->payload(), p->len());
          assert(p->timestamp().timestamp == timestamp3);
          res.push_back(r);
        }
        if (res.size() == 4)
          break;
      }

      assert(res.size() == 4);
      for (int i = 0; i != 4; ++i) {
        assert(res[i].key == test_data[i].key);
        assert(res[i].value == test_data[i].value);
      }
    }
    std::cout << "committing " << last_comitted_offset << std::endl;
    assert(consumer.commit(last_comitted_offset, true) == 0);
    //std::this_thread::sleep_for(6000ms); // 5 s commit flush intervall (how can this be fixed????)
    consumer.stop();

    assert(consumer.topic() == "kspp_test4");
    assert(consumer.partition() == 0);
    consumer.start(); // start from last committed offset

    int64_t first_offset = -1;
    {
      std::vector<record> res;
      std::chrono::time_point<std::chrono::system_clock> end = std::chrono::system_clock::now() + 5000ms;
      while (std::chrono::system_clock::now() < end) {
        auto p = consumer.consume();
        if (p) {
          if (first_offset == -1)
            first_offset = p->offset();
          record r;
          r.key.assign((const char *) p->key_pointer(), p->key_len());
          r.value.assign((const char *) p->payload(), p->len());
          assert(p->timestamp().timestamp == timestamp3);
          res.push_back(r);
        }
        if (res.size() == 4)
          break;
      }
      std::cout << "reading first offset " << first_offset << std::endl;
      assert(first_offset == last_comitted_offset + 1);
      assert(res.size() == 3);
      for (int i = 0; i != 3; ++i) {
        assert(res[i].key == test_data[i + 1].key);
        assert(res[i].value == test_data[i + 1].value);
      }
    }

    /* std::cerr << "sleep forever" << std::endl;
     while (true) {
       std::this_thread::sleep_for(1000ms);
     }*/
  }// test 3
  return 0;
}
