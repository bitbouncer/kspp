SET(LIB_SRCS
  kspp.h
  kspp.cpp
  kstream.h
  ktable.h
  kstream.cpp
  ktable.cpp
  join_processor.h
  )
	
add_library (kspp STATIC ${LIB_SRCS})

