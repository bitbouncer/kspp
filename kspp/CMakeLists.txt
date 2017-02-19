file(GLOB LIB_SRCS
  kspp.h
  type_name.h
  metrics.h
  topology_builder.h
  sinks/*.h
  codecs/*.h
  processors/*.h
  state_stores/*.h
  impl/*.*
  impl/state_stores/*.*
  impl/sources/*.*
  impl/sinks/*.*
  )
	
add_library (kspp STATIC ${LIB_SRCS})

