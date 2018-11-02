#include <cassert>
#include <kspp/state_stores/rocksdb_windowed_store.h>
#include <kspp/impl/serdes/binary_serdes.h>
#include <kspp/utils/env.h>

using namespace std::chrono_literals;

int main(int argc, char **argv) {
  FLAGS_logtostderr = 1;
  google::InitGoogleLogging(argv[0]);

  boost::filesystem::path path = kspp::default_statestore_root();
  path /= "test2_rocksdb_windowed_store";

  if (boost::filesystem::exists(path))
    boost::filesystem::remove_all(path);

  // insert 3 check size
  kspp::rocksdb_windowed_store<int32_t, std::string, kspp::binary_serdes> store(path, 100ms, 10);

  auto t0 = kspp::milliseconds_since_epoch();
  store.insert(std::make_shared<kspp::krecord<int32_t, std::string>>(0, "value0", t0), -1);
  store.insert(std::make_shared<kspp::krecord<int32_t, std::string>>(1, "value1", t0 + 200), -1);
  store.insert(std::make_shared<kspp::krecord<int32_t, std::string>>(2, "value2", t0 + 400), -1);
  assert(store.exact_size() == 3);

  // update existing key with new value
  {
    store.insert(std::make_shared<kspp::krecord<int32_t, std::string>>(2, "value2updated", t0 + 400), -1);
    assert(store.exact_size() == 3);
    auto record = store.get(2);
    assert(record != nullptr);
    assert(record->key() == 2);
    assert(record->value() != nullptr);
    assert(*record->value() == "value2updated");
  }

  // update existing key with new value but old timestamp
  {
    store.insert(std::make_shared<kspp::krecord<int32_t, std::string>>(2, "to_old", t0 + 200), -1);
    assert(store.exact_size() == 3);
    auto record = store.get(2);
    assert(record != nullptr);
    assert(record->key() == 2);
    assert(record->value() != nullptr);
    assert(*record->value() == "value2updated");
  }

  // delete existing key with to old timestamp
  {
    store.insert(std::make_shared<kspp::krecord<int32_t, std::string>>(2, nullptr, t0), -1);
    assert(store.exact_size() == 3);
    auto record = store.get(2);
    assert(record != nullptr);
    assert(record->key() == 2);
    assert(record->value() != nullptr);
    assert(*record->value() == "value2updated");
  }

  // delete existing key with new timestamp
  {
    store.insert(std::make_shared<kspp::krecord<int32_t, std::string>>(2, nullptr, t0 + 700), -1);
    assert(store.exact_size() == 2);
    auto record = store.get(2);
    assert(record == nullptr);
  }

  // test garbage collection
  {
    store.garbage_collect(t0);
    assert(store.exact_size() == 2);
    store.garbage_collect(t0 + 900);
    assert(store.exact_size() == 2);

    // only item 1 should be left
    store.garbage_collect(t0 + 1100);
    assert(store.exact_size() == 1);
    auto record = store.get(1);
    assert(record != nullptr);
    assert(record->key() == 1);
    assert(record->value() != nullptr);
    assert(*record->value() == "value1");

    // this should clear out item 2
    store.garbage_collect(t0 + 1300);
    assert(store.exact_size() == 0);
  }
  return 0;
}


