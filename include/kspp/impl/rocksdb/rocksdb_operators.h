#include <rocksdb/merge_operator.h>
#pragma once

namespace kspp {
  class Int64AddOperator : public rocksdb::AssociativeMergeOperator {
  public:
    static inline bool Deserialize(const rocksdb::Slice &slice, int64_t *value) {
      if (slice.size() != sizeof(int64_t))
        return false;
      memcpy((void *) value, slice.data(), sizeof(int64_t));
      return true;
    }

    static inline void Serialize(int64_t val, std::string *dst) {
      dst->resize(sizeof(int64_t));
      memcpy((void *) dst->data(), &val, sizeof(int64_t));
    }

    static inline bool Deserialize(const std::string &src, int64_t *value) {
      if (src.size() != sizeof(int64_t))
        return false;
      memcpy((void *) value, src.data(), sizeof(int64_t));
      return true;
    }

  public:
    Int64AddOperator() {}
    virtual ~Int64AddOperator() {}

    virtual bool Merge(
        const rocksdb::Slice &key,
        const rocksdb::Slice *existing_value,
        const rocksdb::Slice &value,
        std::string *new_value,
        rocksdb::Logger *logger) const override;

    virtual const char *Name() const override;
  };
} //namespace

