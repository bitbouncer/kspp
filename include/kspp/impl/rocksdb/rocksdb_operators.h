#include <rocksdb/merge_operator.h>
#include <glog/logging.h>
#pragma once

namespace rocksdb {
  std::shared_ptr<rocksdb::MergeOperator> CreateInt64AddOperator();
}

namespace Int64AddOperator {
  inline int64_t Deserialize(const rocksdb::Slice &slice) {
    int64_t value = 0;
    if (slice.size() != sizeof(int64_t)) {
      LOG(ERROR) << "int64 value corruption, size: " << slice.size() << ", expected:" << sizeof(int64_t);
      return value;
    }
    memcpy((void *) &value, slice.data(), sizeof(int64_t));
    return value;
  }

  inline int64_t Deserialize(const std::string &src) {
    int64_t value = 0;
    if (src.size() != sizeof(int64_t)) {
      LOG(ERROR) << "int64 value corruption, size: " << src.size() << ", expected:" << sizeof(int64_t);
      return value;
    }
    memcpy((void *) &value, src.data(), sizeof(int64_t));
    return value;
  }

  inline std::string Serialize(int64_t val) {
    std::string result;
    result.resize(sizeof(int64_t));
    memcpy((void *) result.data(), &val, sizeof(int64_t));
    return result;
  }
}

