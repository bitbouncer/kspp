SET(LIB_SRCS
  kspp_defs.h
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
  codecs/text_codec.h
  codecs/binary_codec.h
  processors/count.h
  processors/transform.h
  processors/join.h
  processors/group_by.h
  state_stores/counter_store.h
  state_stores/state_store.h
  )
	
add_library (kspp STATIC ${LIB_SRCS})

