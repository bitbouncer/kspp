SET(LIB_SRCS
  kspp.h
  kspp.cpp
  kstream.h
  ktable.h
  kstream.cpp
  ktable.cpp
  join_processor.h
  rocksdb_store.h
  rocksdb_store.cpp
  kafka_producer.h
  kafka_producer.cpp
  kafka_consumer.h
  kafka_consumer.cpp
  )
	
add_library (kspp STATIC ${LIB_SRCS})

