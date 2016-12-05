SET(LIB_SRCS
  kspp.h
  kspp.cpp
  kstream.h
  ktable.h
  kstream.cpp
  ktable.cpp
  join_processor.h
  kafka_local_store.h
  kafka_local_store.cpp
  kafka_producer.h
  kafka_producer.cpp
  kafka_consumer.h
  kafka_consumer.cpp
  encoder.h
  )
	
add_library (kspp STATIC ${LIB_SRCS})

