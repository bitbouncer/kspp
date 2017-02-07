#include "kspp.h"
#pragma once

namespace kspp {
template<class KSINK>
int produce(KSINK& sink, const typename KSINK::key_type& key) {
  return sink.produce(std::make_shared<typename KSINK::record_type>(key));
}

template<class KSINK>
int produce(KSINK& sink, const typename KSINK::key_type& key, const typename KSINK::value_type& value) {
  return sink.produce(std::make_shared<typename KSINK::record_type>(key, value));
}

template<class KSINK>
int produce(KSINK& sink, const typename KSINK::value_type& value) {
  return sink.produce(std::make_shared<typename KSINK::record_type>(value));
}

 //TBD bestämm vilket api som är bäst...

template<class K, class V>
int produce(partition_sink<K, V>& sink, const K& key, const V& val) {
  return sink.produce(std::make_shared<krecord<K, V>>(key, std::make_shared<V>(val)));
}

template<class K, class V>
int produce(partition_sink<void, V>& sink, const V& val) {
  return sink.produce(std::make_shared<krecord<void, V>>(std::make_shared<V>(val)));
}

//template<class K, class V>
//int produce(partition_sink<K, V>& sink, const K& key) {
//  return sink.produce(std::move<>(create_krecord<K, V>(key)));
//}
};
