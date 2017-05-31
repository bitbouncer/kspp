#include <cassert>
#include <kspp/state_stores/mem_token_bucket_store.h>

using namespace std::chrono_literals;

template<class T>
size_t exact_size(T& db) {
  size_t sz = 0;
  for (auto& i : db)
    ++sz;
  return sz;
}

int main(int argc, char** argv) {
  {
    // insert 3 check size
    kspp::mem_token_bucket_store<int32_t, int8_t> store(100ms, 2);
    auto t0 = kspp::milliseconds_since_epoch();
    assert(store.consume(0, t0) == true);
    assert(store.consume(1, t0) == true);
    assert(store.consume(2, t0) == true);
    assert(store.size() == 3);
    
    assert(exact_size(store) == 3); // tests iterators

    // consume existing key
    {
      assert(store.consume(2, t0 + 10) == true);
      assert(store.size() == 3);
      auto res = store.get(2);
      assert(res);
      assert(res->key() == 2);
      assert(res->value());
      assert(*res->value() == 0);
      assert(res->event_time() == t0); // less than one item so not incremented
    }

    // consume existing key to fast
    {
      assert(store.consume(2, t0 + 20) == false);
      assert(store.size() == 3);
      auto res = store.get(2);
      assert(res);
      assert(res->key() == 2);
      assert(res->value());
      assert(*res->value() == 0);
      assert(res->event_time() == t0);// less than one item so not incremented
    }

    // consume existing key after one  should be available
    {
      assert(store.consume(2, t0 + 101) == true);
      assert(store.size() == 3);
      auto res = store.get(2);
      assert(res);
      assert(res->key() == 2);
      assert(res->value());
      assert(*res->value()==1);
      assert(res->event_time() == t0 + 101);// more than full time period so reset
    }

    // delete existing key 
    {
      store.del(1);
      assert(store.size() == 2);
      auto res = store.get(1);
      assert(res);
      assert(res->key() == 1);
      assert(res->value());
      assert(*res->value() == 2);
      assert(res->event_time() == -1);
    }
  }
  return 0;
}


