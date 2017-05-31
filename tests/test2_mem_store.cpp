#include <cassert>
#include <kspp/state_stores/mem_store.h>
using namespace std::chrono_literals;

int main(int argc, char** argv) {
  {
  // insert 3 check size
    kspp::mem_store<int32_t, std::string> store("");
    auto t0 = kspp::milliseconds_since_epoch();
    store.insert(std::make_shared<kspp::krecord<int32_t, std::string>>(0, "value0", t0), -1);
    store.insert(std::make_shared<kspp::krecord<int32_t, std::string>>(1, "value1", t0), -1);
    store.insert(std::make_shared<kspp::krecord<int32_t, std::string>>(2, "value2", t0), -1);
    assert(store.size() == 3);

    // update existing key with new value
    {
      store.insert(std::make_shared<kspp::krecord<int32_t, std::string>>(2, "value2updated", t0 + 10), -1);
      assert(store.size() == 3);
      auto record = store.get(2);
      assert(record!=nullptr);
      assert(record->key() == 2);
      assert(record->value() !=nullptr);
      assert(*record->value() == "value2updated");
    }

    // update existing key with new value but old timestamp
    {
      store.insert(std::make_shared<kspp::krecord<int32_t, std::string>>(2, "to_old", t0), -1);
      assert(store.size() == 3);
      auto record = store.get(2);
      assert(record != nullptr);
      assert(record->key() == 2);
      assert(record->value() != nullptr);
      assert(*record->value() == "value2updated");
    }

    // delete existing key with to old timestamp
    {
      store.insert(std::make_shared<kspp::krecord<int32_t, std::string>>(2, nullptr, t0), -1);
      assert(store.size() == 3);
      auto record = store.get(2);
      assert(record != nullptr);
      assert(record->key() == 2);
      assert(record->value() != nullptr);
      assert(*record->value() == "value2updated");
    }

    // delete existing key with new timestamp
    {
      store.insert(std::make_shared<kspp::krecord<int32_t, std::string>>(2, nullptr, t0+30), -1);
      assert(store.size() == 2);
      auto record = store.get(2);
      assert(record == nullptr);
    }
  }
  return 0;
}


