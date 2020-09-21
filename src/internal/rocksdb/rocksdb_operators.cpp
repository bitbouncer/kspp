#ifdef KSPP_ROCKSDB

#include <memory>
#include <rocksdb/env.h>
#include <rocksdb/merge_operator.h>
#include <rocksdb/slice.h>
#include <kspp/internal/rocksdb/rocksdb_operators.h>

// A 'model' merge operator with uint64 addition semantics
// Implemented as an AssociativeMergeOperator for simplicity and example.
namespace rocksdb {

  static inline void PutFixed64(std::string *dst, int64_t val) {
  dst->resize(sizeof(int64_t));
  memcpy((void *) dst->data(), &val, sizeof(int64_t));
  }


  class Int64AddOperator : public rocksdb::AssociativeMergeOperator {
  public:
    virtual bool Merge(const Slice& /*key*/, const Slice* existing_value,
                       const Slice& value, std::string* new_value,
                       Logger* logger) const override {
      assert(new_value);
      int64_t orig_value = 0;
      if (existing_value){
        orig_value = ::Int64AddOperator::Deserialize(*existing_value);
      }
      int64_t operand = ::Int64AddOperator::Deserialize(value);
      int64_t updated_value = orig_value + operand;
      new_value->resize(sizeof(int64_t));
      memcpy((void *) new_value->data(), &updated_value, sizeof(int64_t));
      return true;  // Return true always since corruption will be treated as 0
    }

    virtual const char* Name() const override {
      return "Int64AddOperator";
    }
  };
}


namespace rocksdb {
  std::shared_ptr<rocksdb::MergeOperator> CreateInt64AddOperator(){
    return std::make_shared<Int64AddOperator>();
  }
}
#endif //  KSPP_ROCKSDB
