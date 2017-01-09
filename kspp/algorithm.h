#include "kspp.h"

namespace kspp {
  template<class K, class V>
  std::shared_ptr<krecord<K, V>> create_krecord(const K& k, const V& v) {
    return std::make_shared<krecord<K, V>>(k, std::make_shared<V>(v));
  }

  template<class K, class V>
  std::shared_ptr<krecord<K, V>> create_krecord(const K& k) {
    return std::make_shared<krecord<K, V>>(k);
  }

  template<class K, class V>
  void process_one(const std::vector<std::shared_ptr<partition_source<K, V>>>& sources) {
    for (auto i : sources) {
      i->process_one();
    }
  }

  template<class K, class V>
  bool eof(std::vector<std::shared_ptr<partition_source<K, V>>>& sources) {
    for (auto i : sources) {
      if (!i->eof())
        return false;
    }
    return true;
  }

  template<class K, class V>
  bool eof(std::vector<std::shared_ptr<materialized_partition_source<K, V>>>& sources) {
    for (auto i : sources) {
      if (!i->eof())
        return false;
    }
    return true;
  }

  template<class KSINK>
  int produce(KSINK& sink, const typename KSINK::key_type& key) {
    return sink.produce(std::make_shared<KSINK::record_type>(key));
  }

  template<class KSINK>
  int produce(KSINK& sink, const typename KSINK::key_type& key, const typename KSINK::value_type& value) {
    return sink.produce(std::make_shared<KSINK::record_type>(key, value));
  }

  template<class KSINK>
  int produce(KSINK& sink, const typename KSINK::value_type& value) {
    return sink.produce(std::make_shared<KSINK::record_type>(void, value));
  }

  // TBD bestämm vilket api som är bäst...

  template<class K, class V>
  int produce(partition_sink<K, V>& sink, const K& key, const V& val) {
    return sink.produce(std::move<>(create_krecord<K, V>(key, val)));
  }

  template<class K, class V>
  int produce(partition_sink<void, V>& sink, const V& val) {
    return sink.produce(std::move<>(std::make_shared<krecord<void, V>>(val)));
  }

  template<class K, class V>
  int produce(partition_sink<K, V>& sink, const K& key) {
    return sink.produce(std::move<>(create_krecord<K, V>(key)));
  }
};
