SET(LIB_SRCS
  kspp.h
  kstream_impl.h
  ktable_impl.h
  kafka_source.h
  kafka_sink.h
  kafka_producer.h
  kafka_producer.cpp
  kafka_consumer.h
  kafka_consumer.cpp
  topology_builder.h
  stream_sink.h
  codecs/text_codec.h
  codecs/binary_codec.h
  processors/count.h
  processors/filter.h
  processors/join.h  
  processors/transform.h
  processors/group_by.h
  processors/repartition.h
  state_stores/kv_store.h
  state_stores/counter_store.h
  state_stores/rocksdb_counter_store.h
  state_stores/rocksdb_windowed_counter_store.h
  state_stores/rocksdb_store.h
  state_stores/rocksdb_windowed_store.h
  )
	
add_library (kspp STATIC ${LIB_SRCS})

