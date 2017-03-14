#include <cassert>
#include <kspp/impl/state_stores/mem_token_bucket_store.h>

using namespace std::chrono_literals;

int main(int argc, char** argv) {
  {
    // insert 3 check size
    kspp::mem_token_bucket_store<int32_t> store(100ms, 2);
    auto t0 = kspp::milliseconds_since_epoch();
    assert(store.consume(0, t0) == true);
    assert(store.consume(1, t0) == true);
    assert(store.consume(2, t0) == true);
    assert(store.size() == 3);

    // consume existing key
    {
      assert(store.consume(2, t0 + 10) == true);
      assert(store.size() == 3);
      assert(store.get(2) == 0); // timestamp???
    }

    // consume existing key to fast
    {
      assert(store.consume(2, t0 + 20) == false);
      assert(store.size() == 3);
      assert(store.get(2) == 0);// timestamp???
    }

    // consume existing key after one  should be available
    {
      assert(store.consume(2, t0 + 101) == true);
      assert(store.size() == 3);
      assert(store.get(2) == 1);// timestamp???
    }

    // delete existing key 
    {
      store.del(1);
      assert(store.size() == 2);
      assert(store.get(1) == 2); // we should always have full capacity for non existing keys
    }
  }
  return 0;
}


