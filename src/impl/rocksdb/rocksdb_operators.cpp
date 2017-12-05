#include <kspp/impl/rocksdb/rocksdb_operators.h>
#include <glog/logging.h>

namespace kspp {
  bool Int64AddOperator::Merge(
      const rocksdb::Slice &key,
      const rocksdb::Slice *existing_value,
      const rocksdb::Slice &value,
      std::string *new_value,
      rocksdb::Logger *logger) const {

    // assuming 0 if no existing value
    int64_t existing = 0;
    if (existing_value) {
      if (!Deserialize(*existing_value, &existing)) {
        // if existing_value is corrupted, treat it as 0
        LOG(ERROR) << "Int64AddOperator::Merge existing_value value corruption";
        existing = 0;
      }
    }

    int64_t oper;
    if (!Deserialize(value, &oper)) {
      // if operand is corrupted, treat it as 0
      LOG(ERROR) << "Int64AddOperator::Merge, Deserialize operand value corruption";
      oper = 0;
    }

    Serialize(existing + oper, new_value);
    return true;        // always return true for this, since we treat all errors as "zero".
  }

  const char *Int64AddOperator::Name() const {
    return "Int64AddOperator";
  }
}