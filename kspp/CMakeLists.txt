file(GLOB LIB_SRCS
  kspp.h
  algorithm.h
  topology_builder.h
  codecs/text_codec.h
  codecs/binary_codec.h
  impl/kstream_impl.h
  impl/ktable_impl.h
  processors/*.h
  sinks/kafka_sink.h
  sinks/stream_sink.h
  sinks/kafka_producer.h
  sinks/kafka_producer.cpp
  sources/kafka_source.h
  sources/kafka_consumer.h
  sources/kafka_consumer.cpp
  state_stores/*.h
  )
	
add_library (kspp STATIC ${LIB_SRCS})

