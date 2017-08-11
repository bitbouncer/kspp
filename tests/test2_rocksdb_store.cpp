#include <cassert>
#include <kspp/state_stores/rocksdb_store.h>
#include <kspp/impl/serdes/binary_serdes.h>
#include <kspp/utils.h>

using namespace std::chrono_literals;

int main(int argc, char **argv) {
  boost::filesystem::path path = kspp::utils::default_statestore_directory();
  path /= "test2_rocksdb_store";

  if (boost::filesystem::exists(path))
    boost::filesystem::remove_all(path);

  {
    // insert 3 check size
    kspp::rocksdb_store<int32_t, std::string, kspp::binary_serdes> store(path);
    auto t0 = kspp::milliseconds_since_epoch();
    store.insert(std::make_shared<kspp::krecord<int32_t, std::string>>(0, "value0", t0), -1);
    store.insert(std::make_shared<kspp::krecord<int32_t, std::string>>(1, "value1", t0), -1);
    store.insert(std::make_shared<kspp::krecord<int32_t, std::string>>(2, "value2", t0), -1);
    assert(store.exact_size() == 3);

    // update existing key with new value
    {
      store.insert(std::make_shared<kspp::krecord<int32_t, std::string>>(2, "value2updated", t0 + 10), -1);
      assert(store.exact_size() == 3);
      auto record = store.get(2);
      assert(record != nullptr);
      assert(record->key() == 2);
      assert(record->value() != nullptr);
      assert(*record->value() == "value2updated");
      assert(record->event_time() == t0 + 10);
    }

    //// update existing key with new value but old timestamp
    //{
    //  store.insert(std::make_shared<kspp::ktransaction<int32_t, std::string>>(2, "to_old", t0));
    //  assert(exact_size(store) == 3);
    //  auto record = store.get(2);
    //  assert(record != nullptr);
    //  assert(record->key() == 2);
    //  assert(record->value != nullptr);
    //  assert(*record->value == "value2updated");
    //}

    //// delete existing key with to old timestamp
    //{
    //  store.insert(std::make_shared<kspp::ktransaction<int32_t, std::string>>(2, nullptr, t0));
    //  assert(exact_size(store) == 3);
    //  auto record = store.get(2);
    //  assert(record != nullptr);
    //  assert(record->key() == 2);
    //  assert(record->value != nullptr);
    //  assert(*record->value == "value2updated");
    //}

    // delete existing key with new timestamp
    {
      store.insert(std::make_shared<kspp::krecord<int32_t, std::string>>(2, nullptr, t0 + 30), -1);
      assert(store.exact_size() == 2);
      auto record = store.get(2);
      assert(record == nullptr);
    }
  }
  // cleanup
  boost::filesystem::remove_all(path);
  return 0;
}


