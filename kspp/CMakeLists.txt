file(GLOB LIB_SRCS
  kspp.h
  type_name.h
  metrics.h
  algorithm.h
  topology_builder.h
  sinks/kafka_sink.h
  sinks/stream_sink.h
  codecs/*.h
  processors/*.h
  impl/state_stores/*.*
  impl/sources/*.*
  impl/sinks/*.*
  )
	
add_library (kspp STATIC ${LIB_SRCS})

