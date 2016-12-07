//#include <memory>
//#include <boost/filesystem.hpp>
//#include "kafka_consumer.h"
//#include "kafka_producer.h"
//#include "kafka_local_store.h"
//#pragma once
//
////https://kafka.apache.org/0100/javadoc/org/apache/kafka/streams/processor/TopologyBuilder.html
//
//namespace csi {
// 
//template<class K, class V>
//class stream_record
//{
//  int64_t            event_time;
//  K                  key;
//  std::shared_ptr<V> value;
//};
//
//  template<class K, class V>
//  class kprocessor
//  {
//    void process(stream_record<K, V>& record)
//  };
//
//  template<class K, class V>
//  class kprocessor_context
//  {
//    void forward(K, V);
//    int64_t offset();
//
//    std::vector<std::shared_ptr<kprocessor>> _downstream;
//  };
//
//
//template<class K, class V>
//class ksource
//{
//  std::shared_ptr<kafka_consumer<K,V>> _consumer;
//};
//
//template<class K, class V>
//class ksink
//{
//  std::shared_ptr<kafka_producer<K,V>> _producer;
//};
//
//class kprocessor
//{
//  public:
//  protected:
//  //std::shared_ptr<kafka_consumer> _consumer;
//  //std::shared_ptr<kafka_producer> _producer;
//};
//
//class ktopology_builder
//{
//  public:
//  template<K,V>
//  std::shared_ptr<ksink<K, V>> create_sink() {
//  
//  }
//};
//
//}