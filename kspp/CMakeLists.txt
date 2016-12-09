SET(LIB_SRCS
  kspp_defs.h
  kspp.h
  kstream_impl.h
  ktable_impl.h
  join.h
  state_store.h
  kafka_source.h
  kafka_sink.h
  kafka_producer.h
  kafka_producer.cpp
  kafka_consumer.h
  kafka_consumer.cpp
  binary_encoder.h
  topology_builder.h
  )
	
add_library (kspp STATIC ${LIB_SRCS})

