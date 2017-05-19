file(GLOB LIB_SRCS
  kspp.h
  krecord.h
  ktransaction.h
  type_name.h
  metrics.h
  topology_builder.h
  metrics/*.*
  sinks/*.h
  codecs/*.h
  processors/*.h
  state_stores/*.h
  impl/*.*
  impl/sources/*.*
  impl/sinks/*.*
  impl/serdes/*.*
  impl/hash/*.*
  )

if (ENABLE_AVRO)  
file(GLOB AVRO_SRCS impl/serdes/avro/*.*)  
endif()  
  
add_library (kspp STATIC ${LIB_SRCS} ${AVRO_SRCS})

