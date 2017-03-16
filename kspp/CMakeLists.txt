file(GLOB LIB_SRCS
  kspp.h
  krecord.h
  kevent.h
  type_name.h
  metrics.h
  topology_builder.h
  sinks/*.h
  codecs/*.h
  processors/*.h
  state_stores/*.h
  impl/*.*
  impl/sources/*.*
  impl/sinks/*.*
  impl/serdes/*.*
  )
	
add_library (kspp STATIC ${LIB_SRCS})

