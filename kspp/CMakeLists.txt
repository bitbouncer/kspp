SET(LIB_SRCS
  kspp.h
  algorithm.h
  topology_builder.h
  codecs/text_codec.h
  codecs/binary_codec.h
  impl/kstream_impl.h
  impl/ktable_impl.h
  processors/count.h
  processors/delay.h
  processors/filter.h
  processors/join.h  
  processors/rate_limiter.h
  processors/transform.h
  processors/group_by.h
  processors/repartition.h
  sinks/kafka_sink.h
  sinks/stream_sink.h
  sinks/kafka_producer.h
  sinks/kafka_producer.cpp
  sources/kafka_source.h
  sources/kafka_consumer.h
  sources/kafka_consumer.cpp
  state_stores/kv_store.h
  state_stores/counter_store.h
  state_stores/rocksdb_counter_store.h
  state_stores/rocksdb_windowed_counter_store.h
  state_stores/rocksdb_store.h
  state_stores/rocksdb_windowed_store.h
  )
	
add_library (kspp STATIC ${LIB_SRCS})

