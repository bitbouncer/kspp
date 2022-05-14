#include <kspp/utils/offset_storage_provider.h>
#include <kspp/kspp.h>

int main(int argc, char **argv) {
  auto p = kspp::get_offset_provider("s3://10.1.47.180:9000/kspp-dev/tests/test12_s3.offset");
  p->commit(0, true);
  auto offset = p->start(kspp::OFFSET_STORED);
  assert(offset == 0);
  return 0;
}

