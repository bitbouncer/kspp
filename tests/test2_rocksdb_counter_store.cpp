#include <cassert>
#include <kspp/state_stores/rocksdb_counter_store.h>
#include <kspp/impl/serdes/binary_serdes.h>

static boost::filesystem::path default_directory() {
  if (const char* env_p = std::getenv("KSPP_STATE_DIR")) {
    return boost::filesystem::path(env_p);
  }
  return boost::filesystem::temp_directory_path();
}

template<class T>
size_t exact_size(T& db) {
  size_t sz = 0;
  for (auto& i : db)
    ++sz;
  return sz;
}

int main(int argc, char** argv) {
  boost::filesystem::path path = default_directory();
  path /= "test2_rocksdb_counter_store";

  if (boost::filesystem::exists(path))
    boost::filesystem::remove_all(path);

  {
    // insert
    kspp::rocksdb_counter_store<int32_t, int, kspp::binary_serdes> store(path);
    auto t0 = kspp::milliseconds_since_epoch();
    store.insert(std::make_shared<kspp::krecord<int32_t, int>>(0, 1, t0));
    store.insert(std::make_shared<kspp::krecord<int32_t, int>>(1, 1, t0));
    store.insert(std::make_shared<kspp::krecord<int32_t, int>>(2, 1, t0));
    assert(exact_size(store) == 3);

    // update existing key with new value
    {
      store.insert(std::make_shared<kspp::krecord<int32_t, int>>(2, 1, t0 + 10));
      assert(exact_size(store) == 3);
      auto record = store.get(2);
      assert(record != nullptr);
      assert(record->key == 2);
      assert(record->value != nullptr);
      assert(*record->value == 2);
      // currently we dont store timestams in rocksdb conter store...
      //assert(record->event_time == t0 + 10);
    }

    // update existing key with new value but old timestamp
    // this should be ok since this is an aggregation
    {
      store.insert(std::make_shared<kspp::krecord<int32_t, int>>(2, 2, t0));
      assert(exact_size(store) == 3);
      auto record = store.get(2);
      assert(record != nullptr);
      assert(record->key == 2);
      assert(record->value != nullptr);
      assert(*record->value == 4);
      // this will be broken
      //assert(record->event_time == t0 + 10); // keep biggest timestamp - not latest
    }

    // update existing key with new negative value
    {
      store.insert(std::make_shared<kspp::krecord<int32_t, int>>(0, -2, t0));
      assert(exact_size(store) == 3);
      auto record = store.get(0);
      assert(record != nullptr);
      assert(record->key == 0);
      assert(record->value != nullptr);
      assert(*record->value == -1);
    }

    // broken
    //// delete existing key with to old timestamp
    //// should be forbidden
    //{
    //  store.insert(std::make_shared<kspp::krecord<int32_t, int>>(2, nullptr, t0));
    //  assert(store.size() == 3);
    //  auto record = store.get(2);
    //  assert(record != nullptr);
    //  assert(record->key == 2);
    //  assert(record->value != nullptr);
    //  assert(*record->value == 4);
    //}

    // delete existing key with new timestamp
    {
      store.insert(std::make_shared<kspp::krecord<int32_t, int>>(2, nullptr, t0 + 30));
      assert(exact_size(store) == 2);
      auto record = store.get(2);
      assert(record == nullptr);
    }
  }

  // cleanup
  boost::filesystem::remove_all(path);

  return 0;
}


